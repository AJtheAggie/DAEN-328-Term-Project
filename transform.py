import pandas as pd
import numpy as np


def drop_null_duplicates(df):
    """Clean the data by dropping null values and duplicates."""
    df = df.dropna()
    df = df.drop_duplicates()
    return df


def clean_date(df):
    """Convert date column to datetime and extract year."""
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    return df


def clean_column_name(df, st1, st2):
    """Rename a column from st1 to st2."""
    df = df.rename(columns={st1: st2})
    return df


def normalize_capitalization(df):
    """Normalize all string columns to lowercase."""
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.lower().str.strip()
    return df


def combine_transport_data(dfs):
    """Combine multiple transport dataframes into one."""
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


if __name__ == "__main__":
    d = pd.read_csv("raw_subway_bus.csv")

    d = d[['date',
           'subways_total_estimated_ridership',
           'buses_total_estimated_ridersip']]

    # Cleaning functions
    d = drop_null_duplicates(d)
    d = clean_date(d)
    d = clean_column_name(d, 'buses_total_estimated_ridersip', 'buses_total_estimated_ridership')
    d['date'] = pd.to_datetime(d['date'])

    # Time modification
    d['year'] = d['date'].dt.year

    # Renaming columns to appropriate ones
    d = d[['date',
           'subways_total_estimated_ridership',
           'buses_total_estimated_ridership',
           'year']]

    subways = d[['date', 'subways_total_estimated_ridership', 'year']].copy()
    subways["transport_type"] = "subway"
    subways = subways[['date', 'subways_total_estimated_ridership', 'transport_type', 'year']]
    subways = subways.rename(columns={'subways_total_estimated_ridership': 'ridership'})
    subways = subways[subways["year"].between(2020, 2024)]

    buses = d[['date', 'buses_total_estimated_ridership', 'year']].copy()
    buses['transport_type'] = 'bus'
    buses = buses[['date', 'buses_total_estimated_ridership', 'transport_type', 'year']]
    buses = buses.rename(columns={'buses_total_estimated_ridership': 'ridership'})
    buses = buses[buses["year"].between(2020, 2024)]

    # Yearly gathering of subways
    subways_yearly = subways.groupby('year').agg({'ridership': 'sum'}).reset_index()

    # Yearly gathering of buses
    buses_yearly = buses.groupby('year').agg({'ridership': 'sum'}).reset_index()

    # Ferries
    ferry_df = pd.read_csv("raw_ferry.csv")

    ferry_df["year"] = pd.to_datetime(ferry_df["date"]).dt.strftime("%Y-%m-%d")
    ferry_df['ferry_total_estimated_ridership'] = (
        ferry_df["whitehall_terminal"].astype(float) +
        ferry_df["stgeorge_terminal"].astype(float)
    )
    ferry_df["date"] = pd.to_datetime(ferry_df["date"]).dt.strftime("%Y-%m-%d")
    ferry_df["transport_type"] = "ferry"
    ferry_df["year"] = pd.to_datetime(ferry_df["date"]).dt.year

    ferry_df = drop_null_duplicates(ferry_df)
    ferry_df = clean_date(ferry_df)
    ferry_df = ferry_df[ferry_df["year"].between(2020, 2024)]
    ferry_df = ferry_df[["date", 'ferry_total_estimated_ridership', "transport_type", "year"]]
    ferry_df = ferry_df.rename(columns={'ferry_total_estimated_ridership': 'ridership'})
    ferry_df = ferry_df[['date', 'ridership', 'transport_type', 'year']]

    # Yearly gathering of ferry data
    ferry_yearly = ferry_df.groupby('year').agg({'ridership': 'sum'}).reset_index()
    ferry_yearly = ferry_yearly[['year', 'ridership']]

    # Combine into merged dataframe
    transportation_df = combine_transport_data([subways, buses, ferry_df])

    # Normalize capitalization
    transportation_df = normalize_capitalization(transportation_df)

    transportation_df.to_csv('NewYork_transportations.csv', index=False)

    print("Transformation complete.")