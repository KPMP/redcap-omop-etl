import requests
import re
import sys
import json
import datetime
import pandas as pd
import numpy as np
import argparse
import configparser
import dcc_transforms as dt
import logging

class REDCapETL(object):

    def init(self):
        parser = argparse.ArgumentParser(description='KPMP REDCap ETL (Extract Transform Load)')
        parser.add_argument('-c', '--configfile', dest='config_file', default="config.ini", help='Main config ini file')
        parser.add_argument('-f', '--fake', dest='fake', action='store_true')
        parser.add_argument('-d', '--debug', dest='debug', action='store_true')
        
        self.args = parser.parse_args()

        self.config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        self.config.read(self.args.config_file)

        self.redcap_api_url = self.config.get('redcap','api_url')
        self.redcap_api_token = self.config.get('redcap','api_token')
        self.log_dir = self.config.get('default', 'log_dir', fallback=None)
        if self.log_dir:
            datestring = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            logging.basicConfig(filename=f'{self.log_dir}/redcap-etl-log-{datestring}.log', level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.DEBUG)

        if self.redcap_api_token is None or self.redcap_api_token == '':
            logging.error('Must provide a redcap api token in your config [redcap] api_token')
            raise Exception("Must provide a redcap api token in your config [redcap] api_token")

        self.transform_records = []
        self.unique_fields = set()
        self.filtered_metadata_list = []
        self.transform_metadata = dict()
        self.field_map = None
        self.field_map_dict = dict()
        self.secondary_id_map = dict()
        self.field_map_errors = dict()
    

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
            logging.info(f'redcap export_records args: {redcap_request_args}')
        
        try:
            response = requests.post(self.redcap_api_url, data=redcap_request_args)
        except requests.exceptions.RequestException as e:  
            raise SystemExit(e)
    
        if self.args.debug:
            logging.debug(f'redcap response: {response}')

        self.records = response.json()

        # REDCap currently wont send redcap_data_access_group
        # for EAV record types. Have to pull down and patch in
        self.patch_dag()

        if self.args.debug:
            logging.debug(f'records: {self.records}')

    def patch_dag(self):

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
        expected_project_id = self.config.get('redcap','project_id')
        if int(expected_project_id) != int(self.redcap_project_id):
            raise Exception(f"REDCap project ID validation failed. Expexted {expected_project_id} Actual: {self.redcap_project_id}")
    
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
            logging.info(f'TRANSMIT: {json_result}')
        else:
            try:
                api_endpoint = self.config.get('datalake','api_endpoint')
                api_token = self.config.get('datalake','api_token')
            except Exception as e:
                raise SystemExit(e)

            r = requests.post(url = api_endpoint, json = result, headers={'x-api-token': api_token})
            

            if not r:
                logging.error(f"Failed to transmit data. Got: {r} to {api_endpoint}")
                raise Exception(f"Failed to transmit data. Got: {r} to {api_endpoint}")

            else:
                logging.info(f'successfull posted to {api_endpoint} response: {r}')
            

    def load_field_map(self):
        self.field_map = pd.read_csv(self.config.get('default','field_map_file'))
        self.field_map = self.field_map.where(self.field_map.notnull(), None)
        self.field_map_dict = self.field_map.set_index('field_name').to_dict('index')
        

    def filter_phi(self):
        #nonphi_fields_df = pd.read_csv(self.config.get('default','phifree_fields_file'))
        #nonphi_fields_df['exclude'] = True
        #nonphi_fields_dict = nonphi_fields_df.set_index(['event','field'])['exclude'].to_dict()

        # sanity checks to do
        # look for datelike field values (regex) that have not been cleaned


        new_records = []
        for rec in self.records:
            event_name = rec['redcap_event_name']
            field_name = rec['field_name']
            
            # ef_tup = (event_name, field_name)
            # more dag patch here
            #if ef_tup in nonphi_fields_dict or field_name == 'redcap_data_access_group':
            field_info = self.field_map_dict.get(field_name)
            if not field_info:
                if field_name not in self.field_map_errors:
                    self.field_map_errors[field_name] = 'Missing from field map'
                    logging.error(f'Field {field_name} missing from field map')
                #raise Exception(f"Failed to find field info in field-map for fieldname {field_name}")
                # log, report field error here
            else:
                field_include_status = field_info.get('status')
                restrict_to_events = field_info.get('restrict_to_event_list')
                if field_include_status and field_include_status == 'Include':
                    if not restrict_to_events or restrict_to_events.get(event_name):
                        self.unique_fields.add(field_name)
                        new_records.append(rec)
                elif field_include_status and field_include_status in ['TransformDateYear', 'TransformDate', 'TransformDateTimeSeconds','TransformDateTime']:
                    if rec.get('kpmp_date_cleaned',False) == True:
                        self.unique_fields.add(field_name)
                        new_records.append(rec)
                elif field_name == 'redcap_data_access_group':
                    self.unique_fields.add(field_name)
                    new_records.append(rec)
        self.records = new_records

    def do_transforms(self):

        trans = dt.DateVariableTransform(self)
        if trans:
            trans.process_records()
            self.transform_records.extend(trans.get_transform_records())
            self.transform_metadata[trans.data_namespace] = trans.get_transform_metadata()

        # t1 = dt.InterimSecondaryIDTransform(self)
        # if t1:
        #     t1.process_records()
        #     self.transform_records.extend(t1.get_transform_records())
        #     self.transform_metadata[t1.data_namespace] = t1.get_transform_metadata()
        
        t2 = dt.CalcVariableTransform(self)
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
        
        self.load_field_map()
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