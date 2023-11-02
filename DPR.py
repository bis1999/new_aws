import pandas as pd 
import numpy as np 
from datetime import date
today = date.today()

### Have to make changes

import requests

# Define the URL of the file you want to download
url = "https://www.eia.gov/petroleum/drilling/xls/dpr-data.xlsx"


# Send an HTTP GET request to the URL
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Save the content of the response to a local file
    with open("Dataset/DPR/{}".format(today), "wb") as file:
        file.write(response.content)
    print("Downloaded dpr-data.xlsx successfully.")
else:
    print(f"Failed to download the file. Status code: {response.status_code}")
    
    
    
regions = ['Anadarko Region', 'Appalachia Region', 'Bakken Region',
       'Eagle Ford Region', 'Haynesville Region', 'Niobrara Region',
       'Permian Region']


dts = []
for i in regions:
    df = pd.read_excel("dpr-data.xlsx",sheet_name=i)
    df = df.rename(columns=df.iloc[0])
    df = df.iloc[1:,:]
    df["Region"] = i
    df = df.reset_index(drop=True)
    dts.append(df)   
    
    
all_data = pd.DataFrame()
for i in dts:
       all_data =  pd.concat([all_data,i])
        
cols = ['Date', 'Rig count', 'Production per rig Crude Oil', 'Legacy production change Crude Oil',
       'Total production Crude Oil', 'Production per rig Natural Gas', 'Legacy production change Natural Gas',
       'Total production Natural Gas','Region']