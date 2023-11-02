from tqdm import tqdm 
import pandas as pd 
import numpy as np 
import requests
from pandas import json_normalize
from datetime import date

from datetime import date
global inserting_api



inserting_api = "?api_key=15RJrfyJLw5Zg2uLi07Erq4VholSJuP3Dptyl8vK&"
today = date.today()


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
    


oil_914 = {}

oil_914["North_Dakota"] = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/?frequency=monthly&data[0]=value&facets[series][]=MCRFPND2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
oil_914["New_mexico"] = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/?frequency=monthly&data[0]=value&facets[series][]=MCRFPNM2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
oil_914["Texas"] = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/?frequency=monthly&data[0]=value&facets[series][]=MCRFPTX2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
oil_914["Colorado"] = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/?frequency=monthly&data[0]=value&facets[series][]=MCRFPCO2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
oil_914["Alaska"] = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/?frequency=monthly&data[0]=value&facets[series][]=MCRFPAK2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
oil_914["North_Dakota"] = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/?frequency=monthly&data[0]=value&facets[series][]=MCRFPND2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

oil_914["Oklahoma"] = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/?frequency=monthly&data[0]=value&facets[series][]=MCRFPOK2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

oil_914["Federal Offshore Gulf of Mexico"]="https://api.eia.gov/v2/petroleum/crd/crpdn/data/?frequency=monthly&data[0]=value&facets[series][]=MCRFP3FM2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

NG914 = {}
NG914["Alaska"] = "https://api.eia.gov/v2/natural-gas/prod/sum/data/?frequency=monthly&data[0]=value&facets[series][]=NGM_EPG0_FGW_SAK_MMCFD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
NG914["Colorado"] = "https://api.eia.gov/v2/natural-gas/prod/sum/data/?frequency=monthly&data[0]=value&facets[series][]=NGM_EPG0_FGW_SCO_MMCFD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
NG914["Louisiana"] = "https://api.eia.gov/v2/natural-gas/prod/sum/data/?frequency=monthly&data[0]=value&facets[series][]=NGM_EPG0_FGW_SLA_MMCFD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
NG914["New Mexico"] = "https://api.eia.gov/v2/natural-gas/prod/sum/data/?frequency=monthly&data[0]=value&facets[series][]=NGM_EPG0_FGW_SNM_MMCFD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
NG914["Ohio"]="https://api.eia.gov/v2/natural-gas/prod/sum/data/?frequency=monthly&data[0]=value&facets[series][]=NGM_EPG0_FGW_SOH_MMCFD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

NG914["Oklahoma"] = "https://api.eia.gov/v2/natural-gas/prod/sum/data/?frequency=monthly&data[0]=value&facets[series][]=NGM_EPG0_FGW_SOK_MMCFD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

NG914["Pennsylvania"] = "https://api.eia.gov/v2/natural-gas/prod/sum/data/?frequency=monthly&data[0]=value&facets[series][]=NGM_EPG0_FGW_SPA_MMCFD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
NG914["Texas"] = "https://api.eia.gov/v2/natural-gas/prod/sum/data/?frequency=monthly&data[0]=value&facets[series][]=NGM_EPG0_FGW_STX_MMCFD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
NG914["West Virginia"]="https://api.eia.gov/v2/natural-gas/prod/sum/data/?frequency=monthly&data[0]=value&facets[series][]=NGM_EPG0_FGW_SWV_MMCFD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"


all_urls = list(NG914.values())

dtsets = []
for i in tqdm(all_urls):
    a = generate_csv(i)
    dtsets.append(a)
df = pd.concat(dtsets)

final_ = df.pivot_table(index = "period",columns="process-name", values="value")
final_ = final_[final_.index > '1999-12']
gas_dataset = pd.concat(dtsets)





all_urls_oil = list(oil_914.values())

dtsets = []
for i in tqdm(all_urls_oil):
    a = generate_csv(i)
    dtsets.append(a)
df = pd.concat(dtsets)

final_ = df.pivot_table(index = "period",columns="process-name", values="value")
final_ = final_[final_.index > '1999-12']
oil_dataset = pd.concat(dtsets)


def process_data(dataset):
    data = dataset.pivot_table(values="value",index="period",columns="series-description")
    data = data.reset_index()
    data["period"] = pd.to_datetime(data["period"])
    data["Year"] = data["period"].dt.year
    data["Month"] = data["period"].dt.month
    
    return data

gas_dataset_final = process_data(gas_dataset)
oil_dataset_final = process_data(oil_dataset)

oil_dataset_final.to_csv("Datasets/914_OIL/{}.csv".format(str(today)))
gas_dataset_final.to_csv("Datasets/914_GAS/{}.csv".format(str(today)))

