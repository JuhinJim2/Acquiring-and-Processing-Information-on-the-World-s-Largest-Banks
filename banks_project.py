import requests
import os
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

#Set the working directory here:
os.chdir(r'C:\Users\audre\OneDrive\Desktop\Banking E2E')

#Initialize variables
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = r'C:\Users\audre\OneDrive\Desktop\Banking E2E\Largest_banks_data.csv'
url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
rate_csv = r'C:\Users\audre\OneDrive\Desktop\Banking E2E\exchange_rate.csv'
target_file = "transformed_data.csv"
conn = sqlite3.connect(db_name)

#Initialize a function to log the progress of the code at different stages in a file
log_file = 'code_log.txt'

def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + ',' + message + '\n')

#Initialize a function to extract the data from a webpage
def extract():
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page,'html.parser')

    df = pd.DataFrame(columns=['Name','Market Cap in USD Billions'])

    tables = data.find_all('table')
    rows = tables[2].find_all('tr')

    count = 0

    for row in rows:
        if count < 10:
            col = row.find_all('td')
            if len(col) != 0:
                data_dict = {'Name': col[1].get_text(strip=True),
                             'Market Cap in USD Billions': col[2].get_text(strip=True)}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index = True)
                count+=1
        else:
            break
    
    return df

#Initialize a function to extract the data from a webpage
def transform(data, rate_csv):

    rates = pd.read_csv(rate_csv)

    if 'Rate' not in rates.columns:
        raise ValueError("The 'Rate' column is missing from the rates DataFrame.")

    data['Market Cap in USD Billions'] = data['Market Cap in USD Billions'].str.replace(',', '').astype(float)
    data['EUR'] = round(data['Market Cap in USD Billions'] * rates.loc[rates['Currency'] == 'EUR', 'Rate'].values[0], 2)
    data['GBP'] = round(data['Market Cap in USD Billions'] * rates.loc[rates['Currency'] == 'GBP', 'Rate'].values[0], 2)
    data['INR'] = round(data['Market Cap in USD Billions'] * rates.loc[rates['Currency'] == 'INR', 'Rate'].values[0], 2)

    return data

#Initialize a function to load the data into a CSV file
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file,index=False)
    file_to_load = transformed_data

    return file_to_load

#Initialize a function to load the data into a database
def load_to_db():
     file_to_load = pd.read_csv(target_file)
     file_to_load.to_sql(table_name, conn, if_exists = 'replace', index=False)
    
# Log declared known values
log_progress("Preliminaries complete. Initating ETL process")

# Log the call of the extract() function to extract data
extracted_data = extract()
log_progress("Data extraction complete. Initiating Transformation process")

# Perform Data transformation by calling the transform() function
transformed_data = transform(extracted_data, rate_csv)
log_progress("Data transformation complete. Initiating data transfer to a CSV file")

# Load the transformed data into a CSV file
load_data(target_file, transformed_data)
log_progress("Data loaded into a CSV file, data transfer complete")

# Load the CSV file into an initialized Database
load_to_db()
log_progress("Data is loaded into a database, queries can now be initialize to interact with db")
