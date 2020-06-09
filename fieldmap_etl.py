import json
import numpy as np
import pandas
import re
import sys
from requests import post
from pandas.io.json import json_normalize

url = 'https://redcap.kpmp.org/api/'
request_body = {
    'token': 'BAA21C2068ADF806AA4963D686AD5FE4',
    'content': 'record',
    'format': 'json',
    'type': 'flat',
    'records[0]': '1-4',
    'records[1]': '1-5',
    'records[2]': '3-2',
    'records[3]': '6-3',
    'records[4]': '14-7',
    'fields[0]': 'study_id',
    'forms[0]': 'demographic_information',
    'events[0]': 'enrollment_arm_1',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'true',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

response = post(url, data=request_body)
df_1 = json_normalize(response.json())
header_1 = response.headers
df_1 = df_1.drop(columns=['redcap_repeat_instrument','redcap_repeat_instance'])
df_1 = df_1.drop(columns=['demographic_information_complete','date_demographics','income_support_elder','income_support_minor','last_employment_month'])
df_1 = df_1.replace(r'^\s*$', 0, regex=True)

def checkbox_column_transformer(data_frame, regex, col_id):
    waitlist = list()
    for col_name in list(data_frame.columns):
        try:
            re.match(regex, col_name).group()
            waitlist.append(col_name)
        except AttributeError:
            continue

    for item in waitlist:
        data_frame[item] = data_frame[item].astype(int)
        data_frame[item] = data_frame[item] * int(re.split(regex, item)[1])
    data_frame[f'tmp_{col_id}'] = data_frame[waitlist].values.tolist()
    data_frame = data_frame.drop(columns = waitlist)

    checklist = list()
    for entry in data_frame[f'tmp_{col_id}']:
        checklist.append(list(filter(lambda x: x != 0, entry)))
    data_frame = data_frame.drop(columns = f'tmp_{col_id}')
    data_frame[f'{col_id}'] = checklist
    return data_frame

df_1 = checkbox_column_transformer(df_1, regex=r'^gender_salutation___', col_id='gender_salutation')
df_1 = checkbox_column_transformer(df_1, regex=r'^employment___', col_id='employment')
df_1 = checkbox_column_transformer(df_1, regex=r'^insurance___', col_id='insurance')

df_1.to_csv(sys.argv[1])
