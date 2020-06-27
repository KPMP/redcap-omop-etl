import requests
import re
import sys
import pandas as pd
import numpy as np

url = 'https://redcap.kpmp.org/api/'


def token_printer(auth_file):
    auth_regex = re.compile(r'(?<=TOKEN=)[0-9A-Za-z]+')
    f = open(auth_file, "r")
    token_line = f.readline()
    f.close()
    return auth_regex.search(token_line).group(0)


field_table = pd.read_csv(sys.argv[1])
token = token_printer(sys.argv[2])
form_field_dict = field_table.groupby('Form').Field.apply(list).to_dict()
form_event_dict = field_table.groupby('Form').Event.apply(lambda x: list(set(x))[0]).to_dict()
consent_filter = "[screening_arm_1][consent_complete]='2' and [screening_arm_1][consent]='1'"


def etl(form_event_map, form_field_map):
    extraction_df = pd.DataFrame(columns=['record', 'redcap_event_name', 'field_name', 'value'])
    for k, v in form_event_map.items():
        form_filter = f"[{v}][{k}_complete]='2'"
        api_filter = consent_filter + ' and ' + form_filter
        data_event = \
            {
                'token': token,
                'content': 'record',
                'format': 'json',
                'type': 'eav',
                'forms': k,
                'events': v,
                'rawOrLabel': 'raw',
                'rawOrLabelHeaders': 'raw',
                'exportCheckboxLabel': 'true',
                'exportSurveyFields': 'false',
                'exportDataAccessGroups': 'false',
                'returnFormat': 'json',
                'filterLogic': api_filter
            }
        response = requests.post(url, data=data_event)
        # header = response.headers
        df = pd.json_normalize(response.json())
        extraction = df[df['field_name'].isin(form_field_map[k])]
        extraction_df = pd.concat([extraction_df, extraction])
    return extraction_df


extraction_df = etl(form_event_dict, form_field_dict)
extraction_df.to_csv(sys.argv[3])

