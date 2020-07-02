import requests
import re
import sys
import datetime
import pandas as pd
import numpy as np
import argparse
import configparser

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

        self.calculated_field_events = set()
    

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
        if self.args.debug:
            print(f'records: {self.records}')

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
    
    def transmit(self):

        result = dict(
            records=self.records,
            redcap_project_id=self.redcap_project_id,
            extraction_run_datetime=datetime.datetime.now()
            )
        
        include_metadata = self.config.getboolean('redcap','include_metadata', fallback=False)
        if include_metadata:
            result['metadata'] = self.metadata
        
        if self.args.fake:
            print(f'TRANSMIT: {result}')
        else:
            try:
                api_endpoint = self.config.get('datalake','endpoint')
                api_token = self.config.get('datalake','token')
            except Exception as e:
                raise SystemExit(e)

            r = requests.post(url = api_endpoint, data = result, headers={'x-api-token': api_token})

            if not r:
                raise f"Failed to transmit data {r}"


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
            if ef_tup in nonphi_fields_dict or event_name in self.calculated_field_events:
                new_records.append(rec)
        self.records = new_records

    def do_transforms(self):
        pass


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