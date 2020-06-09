import requests
import re
import sys
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize

checkbox_regex = re.compile(r'\w+___\d+')
url = 'https://redcap.kpmp.org/api/'


def answer_parser(df):
    df = df.replace(r'^\s*$', '0', regex=True)
    melt_df = df.reset_index().melt(id_vars='study_id')
    checkbox_description = melt_df['variable'] == \
                           [y.group(0) if y is not None else 0
                            for y in [checkbox_regex.match(x) for x in melt_df['variable']]]
    melt_df['if_checkbox'] = np.where(checkbox_description, 1, 0)
    melt_df['if_choice'] = np.where(checkbox_description, 0, 1)

    melt_df['checkbox_code'] = melt_df['variable'] * melt_df['if_checkbox'] * melt_df['value'].astype(int)
    melt_df['choice_code'] = melt_df['if_choice'] * melt_df['value'].astype(int)

    melt_df['checkbox_code'] = melt_df['checkbox_code'].replace(r'\w+___(?=\d)', '', regex=True)
    melt_df['variable'] = melt_df['variable'].replace(r'___\d+', '', regex=True)
    melt_df['checkbox_code'] = melt_df['checkbox_code'].replace(r'^\s*$', '0', regex=True)

    melt_df['answer_code'] = melt_df['checkbox_code'].astype(int) + melt_df['choice_code']

    melt_df = melt_df.drop(columns=['value', 'if_checkbox', 'if_choice', 'checkbox_code', 'choice_code'])
    melt_df = melt_df[melt_df.answer_code != 0]
    melt_df = melt_df.rename(columns={'variable': 'question_description'})
    melt_df = melt_df.sort_values(by=['study_id', 'question_description'])
    return melt_df.reset_index(drop=True)


data_event_1 = \
{
    'token': 'BAA21C2068ADF806AA4963D686AD5FE4',
    'content': 'record',
    'format': 'json',
    'type': 'flat',
    'fields[0]': 'study_id',
    'forms[0]': 'new_participant',
    'forms[1]': 'consent',
    'events[0]': 'screening_arm_1',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'true',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

data_event_2 = \
{
    'token': 'BAA21C2068ADF806AA4963D686AD5FE4',
    'content': 'record',
    'format': 'json',
    'type': 'flat',
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

response = requests.post(url, data=data_event_1)
header_1 = response.headers
df_1 = json_normalize(response.json())

response = requests.post(url, data=data_event_2)
header_2 = response.headers
df_2 = json_normalize(response.json())

consent_df = df_1.loc[(df_1['consent'] == '1') &
                      (df_1['consent_complete'] == '2')][['study_id', 'consent', 'consent_complete']]\
             .set_index('study_id')

df_1_pick_column_regex = re.compile(r'(study_id)|(np_age$)|(np_gender)'
                                    r'|(np_race(.*))|(np_latino_yn)|(np_language(.*))'
                                    r'|(new_participant_complete)|(consent$)|(consent_complete)')

df_1 = df_1.loc[(df_1['new_participant_complete'] == '2')].set_index('study_id').reindex(consent_df.index).dropna()
df_1 = df_1.drop(columns=list(set(df_1.columns)
                              - {y.group(0)
                                 for y in [df_1_pick_column_regex.match(x) for x in df_1.columns]
                                 if y is not None}))

df_2_drop_column_regex = re.compile(r'(redcap(.*))|(last_employment_month)|(date_demographics)')
df_2 = df_2.loc[(df_2['demographic_information_complete'] == '2')].set_index('study_id')
df_2 = df_2.merge(consent_df, how='inner', on='study_id')
df_2 = df_2.drop(columns=list(y.group(0) for y in
                              [df_2_drop_column_regex.match(x) for x in df_2.columns]
                              if y is not None))

df_1 = answer_parser(df_1)
df_2 = answer_parser(df_2)

integrate_df = pd.concat([df_1, df_2]).drop_duplicates()
integrate_df = integrate_df.sort_values(by=['study_id', 'question_description']).reset_index(drop=True)

integrate_df.to_csv(sys.argv[1], index=False)
