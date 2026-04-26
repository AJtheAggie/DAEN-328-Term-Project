import requests
import pandas as pd
from io import StringIO


def ingest_data(api_url, transport_type, limit):
    """Fetches CSV data from the provided API URL using requests."""
    try:
        response = requests.get(api_url, params={"$limit": limit, "$order": ":id"})
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        df['transport_type'] = transport_type
        return df
    except Exception as e:
        print(f"Error ingesting {transport_type} data: {e}")
        return None


def ingest_soda(base_url, limit):
    """Fetches JSON data from a SODA API endpoint using requests."""
    if "/resource/" not in base_url:
        base_url = base_url.replace("/api/views/", "/resource/").split("?")[0]

    response = requests.get(f"{base_url}.json", params={"$limit": limit})
    response.raise_for_status()
    df = pd.DataFrame(response.json())
    return df


if __name__ == "__main__":

    # Buses and subways data
    url = "https://data.ny.gov/resource/vxuj-8kew"
    d = ingest_soda(url, limit=6000)
    d.to_csv("raw_subway_bus.csv", index=False)

    # Ferry data
    ferry_api_url = "https://data.cityofnewyork.us/resource/6eng-46dm.csv"
    ferry_df = ingest_data(ferry_api_url, "ferry", limit=6000)
    ferry_df.to_csv("raw_ferry.csv", index=False)

    print("Extraction complete.")