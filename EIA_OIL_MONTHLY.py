import pandas as pd

from datetime import date

import glob
import os
from tqdm import tqdm 

from stqdm import stqdm
import pickle

import base64
## Weekly Update in Thursday 

## Oil Weekly 




import pandas as pd 
import numpy as np
import requests
from pandas import json_normalize
import pickle
from datetime import date



global inserting_api



inserting_api = "?api_key=15RJrfyJLw5Zg2uLi07Erq4VholSJuP3Dptyl8vK&"

def replace_char(string, char_to_replace, new_string):
    result = ""
    for char in string:
        if char == char_to_replace:
            result += new_string
        else:
            result += char

    return result

def generate_csv(url,save = False,Head_cat=None,sub_cat = None,return_ = True):
    url_with_api = replace_char(url,"?",inserting_api)
    r = requests.get(url_with_api)
    json_data = r.json()
    df = json_normalize(json_data["response"]["data"])
    if save == True:
        df.to_csv("Series/{}_{}.csv".format(Head_cat,sub_cat))
    
    if return_ == True:
        return df





with open('monthly_oil_apis.pkl', 'rb') as f:
   oils_urls = pickle.load(f)

df = []
for i in tqdm(oils_urls):
    a = generate_csv(i)
    df.append(a)
comdt = pd.concat(df)
oil_data= pd.pivot_table(data = comdt,values="value",columns="series-description",index = "period")
oil_data = oil_data.reset_index()
oil_data["period"] = pd.DatetimeIndex(oil_data["period"])
oil_data["month"] = oil_data["period"].dt.month
oil_data["year"] =  oil_data["period"].dt.year
today = date.today()
oil_data.to_csv('Datasets/eia_oil_montly/{}.csv'.format(str(today)))
