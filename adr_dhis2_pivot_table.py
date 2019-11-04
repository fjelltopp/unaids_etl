import json
import os

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin

url = "https://hmis2.health.go.ug/hmis2/api/"
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")


def get_metadata(from_pickle=False):
    global category_combos
    global data_elements
    if from_pickle:
        category_combos = pd.read_pickle("build/category_combos.pickle")
        data_elements = pd.read_pickle("build/data_elements.pickle")
        return
    if not os.path.exists("build"):
        os.makedirs("build")
    cc_resource = "categoryOptionCombos?paging=false&fields=id,name"
    de_resource = "dataElements?paging=false&fields=id,name"
    r_cc = requests.get(urljoin(url, cc_resource), auth=HTTPBasicAuth(username, password))
    r_de = requests.get(urljoin(url, de_resource), auth=HTTPBasicAuth(username, password))
    cc_list = json.loads(r_cc.text)['categoryOptionCombos']
    de_list = json.loads(r_de.text)['dataElements']
    category_combos = pd.DataFrame(cc_list)
    data_elements = pd.DataFrame(de_list)
    category_combos.to_pickle("build/category_combos.pickle")
    data_elements.to_pickle("build/data_elements.pickle")


if __name__ == '__main__':
    with open("inputs/uga/anc.json") as f:
        json_response = json.loads(f.read())

    get_metadata(from_pickle=True)
    df = pd.DataFrame(json_response['dataValues'])
    df['dataElement'] = df['dataElement'].replace(data_elements.set_index('id')['name'])
    df['categoryOptionCombo'] = df['categoryOptionCombo'].replace(category_combos.set_index('id')['name'])
    df['indicatorName'] = df['dataElement'] + '$' +  df['categoryOptionCombo']
    output_df = df[['orgUnit', 'period', 'indicatorName', 'value']]
    output_df['value'] = output_df['value'].astype(int)
    pivot = output_df[['indicatorName', 'value']].pivot(columns='indicatorName', values='value')
    out = pd.concat([output_df[['orgUnit', 'period']], pivot], axis=1)
    out.to_csv("output/uga/anc.csv", index=None)
