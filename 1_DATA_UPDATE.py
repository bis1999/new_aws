import streamlit as st
import pandas as pd

from datetime import date
import plotly.graph_objects as go
import glob
import os
from tqdm import tqdm 

from stqdm import stqdm
import pickle

import base64

import streamlit as st



def download_link(df, filename, text):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # Encode to base64
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{text}</a>'
    return href


st.set_page_config(
    page_title="Commodity Analysis",
    page_icon="📊"
)

st.title("Data Update Page")
st.sidebar.success("Select a page above.")





# Using Markdown to change text color

################################################EIA MONTHLY


st.subheader(':green[EIA Gas Weekly]')

datasets = glob.glob('Datasets/eia_ng_weekly/*.csv')
dates = [i.split("/")[-1].split(".")[0]for i in datasets]

max_date = max(dates)


eia_gas=pd.read_csv("Datasets/eia_ng_weekly/{}.csv".format(max_date))

latest_date = max(eia_gas['Period'])
st.write("Latest Data Available: {} ".format(latest_date))
tmp_download_link = download_link(eia_gas, 'EIA_GAS_WEEKLY.csv', 'Click here to download your data!')
st.markdown(tmp_download_link, unsafe_allow_html=True)



st.subheader(':green[EIA OIL MONTHLY]')

datasets = glob.glob('Datasets/eia_oil_monthly/*.csv')
dates = [i.split("/")[-1].split(".")[0]for i in datasets]

max_date = max(dates)


eia_gas=pd.read_csv("Datasets/eia_oil_monthly/{}.csv".format(max_date))

latest_date = max(eia_gas['period'])
st.write("Latest Data Available: {} ".format(latest_date))
tmp_download_link = download_link(eia_gas, 'EIA_GAS_WEEKLY.csv', 'Click here to download your data!')
st.markdown(tmp_download_link, unsafe_allow_html=True)





st.subheader(':green[EIA OIL Weekly]')

datasets = glob.glob('Datasets/eia_oil_weekly/*.csv')
dates = [i.split("/")[-1].split(".")[0]for i in datasets]

max_date = max(dates)


eia_gas=pd.read_csv("Datasets/eia_oil_weekly/{}.csv".format(max_date))

latest_date = max(eia_gas['period'])
st.write("Latest Data Available: {} ".format(latest_date))
tmp_download_link = download_link(eia_gas, 'EIA_GAS_WEEKLY.csv', 'Click here to download your data!')
st.markdown(tmp_download_link, unsafe_allow_html=True)




st.subheader(':green[NOAA STATIONS]')

datasets = glob.glob('Datasets/noaa_stations/*.csv')
dates = [i.split("/")[-1].split(".")[0]for i in datasets]

max_date = max(dates)


eia_gas=pd.read_csv("Datasets/noaa_stations/{}.csv".format(max_date))

latest_date = max(eia_gas['DATE'])
st.write("Latest Data Available: {} ".format(latest_date))
#tmp_download_link = download_link(eia_gas, 'EIA_GAS_WEEKLY.csv', 'Click here to download your data!')
#st.markdown(tmp_download_link, unsafe_allow_html=True)


st.subheader(':green[914 Data]')

datasets = glob.glob('Datasets/914_OIL/*.csv')
dates = [i.split("/")[-1].split(".")[0]for i in datasets]

max_date = max(dates)


eia_gas=pd.read_csv("Datasets/914_OIL/{}.csv".format(max_date))

latest_date = max(eia_gas['period'])
st.write("Latest Data Available: {} ".format(latest_date))
tmp_download_link = download_link(eia_gas, 'EIA_GAS_WEEKLY.csv', 'Click here to download your data!')
st.markdown(tmp_download_link, unsafe_allow_html=True)

      
        
