import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from urllib.parse import quote
import sqlite3


if __name__ == "__main__":
    transportation_df = pd.read_csv('NewYork_transportations.csv')


    #Connect (establish)
    conn = sqlite3.connect('NY_transportations.db')

    transportation_df.to_sql(name='NY_cleaned', con=conn, if_exists='replace', index=False)

    print("Pushed")


    #connect to the database
    conn = sqlite3.connect('NY_transportations.db')
    df_c = pd.read_sql('SELECT * FROM NY_cleaned LIMIT 30', conn)

    print(df_c)

    #Safety
    subways = transportation_df[transportation_df['transport_type'] == 'subway'].copy()
    buses = transportation_df[transportation_df['transport_type'] == 'bus'].copy()
    ferry_df = transportation_df[transportation_df['transport_type'] == 'ferry'].copy()

    # plt.figure(figsize=(12, 6))

    # plt.plot(subways['date'], subways['ridership'],
    #          label='Subways')

    # plt.plot(buses['date'], buses['ridership'],
    #          label='Buses')

    # plt.plot(ferry_df['date'], ferry_df['ridership'],
    #          label='Ferries')

    # plt.title("NYC Transit Ridership Over Time (Daily)")
    # plt.xlabel("Date")
    # plt.ylabel("Ridership")
    # plt.legend()

    # plt.show()
