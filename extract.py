import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from urllib.parse import quote


#This function ingests data from the provided API link.
def ingest_data(api_url, transport_type, limit):
  
    try:
        df = pd.read_csv(f"{api_url}?$limit={limit}&$order=:id")
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        df['transport_type'] = transport_type
        return df
    except Exception as e:
        print(f"Error ingesting {transport_type} data: {e}")
        return None



#This function ingests the data that involves SODA version/config
def ingest_soda(base_url, limit):

    if "/resource/" not in base_url:
        base_url = base_url.replace("/api/views/", "/resource/").split("?")[0]

    url = f"{base_url}.json?$limit={limit}"

    df = pd.read_json(url)
    return df

#Testing
if __name__ == "__main__":

    #This is the buses and the subways data
    url = "https://data.ny.gov/resource/vxuj-8kew.csv"
    d = ingest_soda(url, limit=6000)
    d.to_csv("raw_subway_bus.csv", index=False)


    #This is the ferry data 
    ferry_api_url = "https://data.cityofnewyork.us/resource/6eng-46dm.csv"
    ferry_df = ingest_data(ferry_api_url, "ferry", limit=6000)
    ferry_df.to_csv("raw_ferry.csv", index=False)

    print("Extraction complete.")
