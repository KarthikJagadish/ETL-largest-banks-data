from datetime import datetime
import requests
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
import numpy as np

def log_progress(message):
    ''' This function logs the mentioned message of a given stage 
    of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d %H-%M-%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as f: 
        f.write(str(timestamp + " : " + message + "\n"))
    f.close()

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df = pd.DataFrame(columns=table_attribs)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    table = tables[0]

    for tr in table.select('tr:has(td)'):
        tds = [td.get_text(strip=True) for td in tr.select('td')]
        name = tds[1]
        mc_usd_billion = tds[2]
        data = pd.DataFrame([[name, mc_usd_billion]], columns=table_attribs)
        df = pd.concat([df, data])
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exch_df = pd.read_csv(csv_path)
    transformed_df = df
    #print(float(exch_df.loc[exch_df['Currency'] == 'GBP']['Rate']))
    transformed_df['MC_GBP_Billion'] = df['MC_USD_Billion'].apply(lambda x: float(x) * float(exch_df.loc[exch_df['Currency'] == 'GBP']['Rate'].iloc[0]))
    transformed_df['MC_EUR_Billion'] = df['MC_USD_Billion'].apply(lambda x: float(x) * float(exch_df.loc[exch_df['Currency'] == 'EUR']['Rate'].iloc[0]))
    transformed_df['MC_INR_Billion'] = df['MC_USD_Billion'].apply(lambda x: float(x) * float(exch_df.loc[exch_df['Currency'] == 'INR']['Rate'].iloc[0]))
    transformed_df = transformed_df.round(decimals = 2)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index = False, float_format='%.2f')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    ''' Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''
    print("Result for: ", query_statement)
    df = pd.read_sql(query_statement, sql_connection)
    print(df)


log_file = "code_log.txt"
exch_rate_csv_file = "/home/project/exchange_rate.csv"
output_csv_file = "/home/project/Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
query1 = "SELECT * FROM Largest_banks;"
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks;"
query3 = "SELECT Name from Largest_banks LIMIT 5"

log_progress("Preliminaries complete. Initiating ETL process")

extracted_df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

transformed_df = transform(extracted_df, exch_rate_csv_file)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(transformed_df, output_csv_file)
log_progress("Data saved to CSV file")

conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(transformed_df, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")


run_query(query1, conn)
run_query(query2, conn)
run_query(query3, conn)
log_progress("Process Complete")

conn.close()
log_progress("Server Connection closed")
