import requests
import re
import sys
import json
import datetime
import pandas as pd
import numpy as np
import argparse
import configparser

from dcc_transforms import TestCalcVariableTransform, TestRandomSecondaryIDTransform

class REDCapETL(object):

    def init(self):
        parser = argparse.ArgumentParser(description='KPMP REDCap ETL (Extract Transform Load)')
        parser.add_argument('-c', '--configfile', dest='config_file', default="config.ini", help='Main config ini file')
        parser.add_argument('-f', '--fake', dest='fake', action='store_true')
        parser.add_argument('-d', '--debug', dest='debug', action='store_true')
        
        self.args = parser.parse_args()

        self.config = configparser.ConfigParser()
        self.config.read(self.args.config_file)

        self.redcap_api_url = self.config.get('redcap','api_url')
        self.redcap_api_token = self.config.get('redcap','api_token')

        if self.redcap_api_token is None or self.redcap_api_token == '':
            raise Exception("Must provide a redcap api token in your config [redcap]api_token")

        self.transform_records = []
        self.unique_fields = set()
        self.filtered_metadata_list = []
        self.transform_metadata = dict()
    

    def get_records(self):
        """
        Pull down all records that conform with the defined api_filter. 
        When using eav, currently exportDataAccessGroups does not work.
        """

        api_filter = self.config.get('redcap','api_filter', fallback=None)
        
        redcap_request_args = \
            {
                'token': self.redcap_api_token,
                'content': 'record',
                'format': 'json',
                'type': 'eav',
                'rawOrLabel': 'raw',
                'rawOrLabelHeaders': 'raw',
                'exportCheckboxLabel': 'true',
                'exportSurveyFields': 'false',
                'exportDataAccessGroups': 'true',
                'returnFormat': 'json',
                'filterLogic': api_filter
            }

        if self.args.debug:
            print(f'redcap export_records args: {redcap_request_args}')
        
        try:
            response = requests.post(self.redcap_api_url, data=redcap_request_args)
        except requests.exceptions.RequestException as e:  
            raise SystemExit(e)
    
        if self.args.debug:
            print(f'redcap response: {response}')

        self.records = response.json()

        # REDCap currently wont send redcap_data_access_group
        # for EAV record types. Have to pull down and patch in
        self.patch_dag()

        if self.args.debug:
            print(f'records: {self.records}')

    def patch_dag(self):

        print('PATCH DAG ENTER')
        api_filter = self.config.get('redcap','api_filter', fallback=None)
        redcap_request_args = \
            {
                'token': self.redcap_api_token,
                'content': 'record',
                'format': 'json',
                'type': 'flat',
                'fields': ['study_id'],
                'events': ['screening_arm_1'],
                'exportDataAccessGroups': 'true',
                'returnFormat': 'json',
                'filterLogic': api_filter
            }

        try:
            response = requests.post(self.redcap_api_url, data=redcap_request_args)
        except requests.exceptions.RequestException as e:  
            raise SystemExit(e)

        dag_records = response.json()

        # stuff dag in as additional field in eav
        for rec in dag_records:
            self.records.append(
                dict(
                    record=rec.get('study_id'), 
                    redcap_event_name=rec.get('redcap_event_name'), 
                    redcap_repeat_instance="", 
                    redcap_repeat_instrument="", 
                    field_name='redcap_data_access_group', 
                    value=rec.get('redcap_data_access_group')
                    ))
            # example eav export row that we are mimicking
            #{"record": "1-4", "redcap_event_name": "biopsy_suite_arm_1", "redcap_repeat_instrument": "", "redcap_repeat_instance": "", "field_name": "bp_kit_nbr_a", "value": "KL-0013065"}

    def get_metadata(self):
        
        redcap_api_data = {
            'token': self.redcap_api_token,
            'content': 'metadata',
            'format': 'json'
        }
        
        redcap_api_result = requests.post(self.redcap_api_url, redcap_api_data)
        self.metadata = redcap_api_result.json()

    def get_project_info(self):

        redcap_api_data = {
            'token': self.redcap_api_token,
            'content': 'project',
            'format': 'json'
        }
        
        redcap_api_result = requests.post(self.redcap_api_url, redcap_api_data)
        self.project_info = redcap_api_result.json()
        self.redcap_project_id = self.project_info.get('project_id')
    
    def filtered_metadata(self):

        if not self.filtered_metadata_list:
            for md in self.metadata:
                if md.get('field_name') in self.unique_fields:
                    self.filtered_metadata_list.append(md)

        return self.filtered_metadata_list

    def transmit(self):

        result = dict(
            redcap_records=self.records,
            transform_records=self.transform_records,
            redcap_project_id=self.redcap_project_id,
            extraction_run_datetime=datetime.datetime.now().isoformat()
            )
        
        include_metadata = self.config.getboolean('redcap','include_metadata', fallback=False)
        if include_metadata:
            result['redcap_metadata_filtered'] = self.filtered_metadata()
            result['transform_metadata'] = self.transform_metadata
        
        json_result = json.dumps(result)

        if self.args.fake:
            print(f'TRANSMIT: {json_result}')
        else:
            try:
                api_endpoint = self.config.get('datalake','api_endpoint')
                api_token = self.config.get('datalake','api_token')
            except Exception as e:
                raise SystemExit(e)

            r = requests.post(url = api_endpoint, json = result, headers={'x-api-token': api_token})
            print(r.json())
            #r = True

            if not r:
                raise Exception(f"Failed to transmit data {r} to {api_endpoint}")
            else:
                print(f'successfull posted to {api_endpoint}')
            


    def filter_phi(self):
        nonphi_fields_df = pd.read_csv(self.config.get('default','phifree_fields_file'))
        nonphi_fields_df['exclude'] = True
        nonphi_fields_dict = nonphi_fields_df.set_index(['event','field'])['exclude'].to_dict()

        # sanity checks to do
        # validate field is present in form in metadata
        # validate form in event via form_event_mapping

        new_records = []
        for rec in self.records:
            event_name = rec['redcap_event_name']
            field_name = rec['field_name']
            ef_tup = (event_name, field_name)
            # more dag patch here
            if ef_tup in nonphi_fields_dict or field_name == 'redcap_data_access_group':
                self.unique_fields.add(field_name)
                new_records.append(rec)
        self.records = new_records

    def do_transforms(self):
        t1 = TestRandomSecondaryIDTransform(self)
        if t1:
            t1.process_records()
            self.transform_records.extend(t1.get_transform_records())
            self.transform_metadata[t1.data_namespace] = t1.get_transform_metadata()
        
        t2 = TestCalcVariableTransform(self)
        if t2:
            t2.process_records()
            self.transform_records.extend(t2.get_transform_records())
            self.transform_metadata[t2.data_namespace] = t2.get_transform_metadata()

        #self.transform_records.extend(TestCalcVariableTransform().process_records(self))


        
    def run(self):
        self.init()
        self.get_project_info()
        self.get_metadata()
        self.get_records()
        
        # 
        self.do_transforms()

        # always restrict to the safe phi free list last
        self.filter_phi()

        self.transmit()



def main():
    etl = REDCapETL()
    etl.run()


if __name__ == "__main__":
    main()