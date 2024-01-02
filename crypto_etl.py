
#defining the required libraries
import pandas as pd
import requests
import s3fs
import json
from datetime import datetime


#defining the python function for extracting, transforming and loading data
def dag_crpyto_etl():
    url = "https://api.coincap.io/v2/assets"            #defining a variable to store the static URL
    header={"Conteny-Type":"application/json",          #setting the header format as json so that the request will pull the data headers in json
            "Accept-Encoding":"deflate"}                #this defines the encoding method

    response = requests.get(url,headers=header)         #pulling data from url in the format specified using the requests library

    response_data = response.json()                     #converting the extracted data in json format

    df_crypto = pd.json_normalize(response_data, 'data')    #normalizing the json data to convert it into a dataframe 

    df_crypto.to_csv("s3://shubhamk-airflow-etl-bucket/Cryptocurrency_data.csv")        #loading the extracted and transformed data into an S3 bucket