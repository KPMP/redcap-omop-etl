import json
import requests
import re
import sys
import pandas as pd
import numpy as np
from io import BytesIO
from pandas.io.json import json_normalize

url = 'https://redcap.kpmp.org/api/'

buffer = BytesIO()
data = {
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

response = requests.post(url, data=data)
header_1 = response.headers
df_1 = json_normalize(response.json())
df_1 = df_1.drop(columns=['redcap_event_name','redcap_repeat_instrument','redcap_repeat_instance'])
df_1 = df_1.drop(columns=['demographic_information_complete','date_demographics',
                          'income_support_elder','income_support_minor','last_employment_month'])
df_1 = df_1.replace(r'^\s*$', 0, regex=True)

checkbox_regex = re.compile(r'\w+___\d+')

df_2 = df_1.melt(id_vars='study_id')
checkbox_description = df_2['variable'] == \
                       [y.group(0) if y is not None else 0
                        for y in [checkbox_regex.match(x) for x in df_2['variable']]]
df_2['if_checkbox'] = np.where(checkbox_description, 1, 0)
df_2['if_choice'] = np.where(checkbox_description, 0, 1)
df_2['checkbox_code'] = df_2['variable'] * df_2['if_checkbox'] * df_2['value'].astype(int)
df_2['choice_code'] = df_2['if_choice'] * df_2['value'].astype(int)

df_2['checkbox_code'] = df_2['checkbox_code'].replace(r'\w+___(?=\d)', '', regex=True)
df_2['variable'] = df_2['variable'].replace(r'___\d+', '', regex=True)

df_2['checkbox_code'] = df_2['checkbox_code'].replace(r'^\s*$', 0, regex=True)
df_2['answer_code'] = df_2['checkbox_code'].astype(int) + df_2['choice_code']

df_2 = df_2.drop(columns=['value', 'if_checkbox', 'if_choice', 'checkbox_code', 'choice_code'])
df_2 = df_2[df_2.answer_code != 0]
df_2 = df_2.rename(columns={'variable': 'question_description'})
df_2 = df_2.sort_values(by=['study_id'])
df_2 = df_2.reset_index(drop=True)

df_2.to_csv(sys.argv[1], index=False)
