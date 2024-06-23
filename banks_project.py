import requests
import os
import sqlite3
import pandas as pd
import numpy as np
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
                             'Market Cap in USB Billions': col[2].get_text(strip=True)}
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

    data['EUR'] = round(data['Market Cap in USD Billions'] * rates.loc[rates['Currency'] == 'EUR', 'Rate'].values[0], 2)
    data['GBP'] = round(data['Market Cap in USD Billions'] * rates.loc[rates['Currency'] == 'GBP', 'Rate'].values[0], 2)
    data['INR'] = round(data['Market Cap in USD Billions'] * rates.loc[rates['Currency'] == 'INR', 'Rate'].values[0], 2)

    return data

# Log declared known values
log_progress("Preliminaries complete. Initating ETL process")

# Log the call of the extract() function
extracted_data = extract()
log_progress("Data extraction complete. Initiating Transformation process")

# Perform Data transformation by calling the transform() function
transformed_data = transform(extracted_data, rate_csv)
log_progress("Data transformation complete. Initiating data transfer to a CSV file")

print(transformed_data)
# Load the extracted data
# df.to_csv(csv_path)


# #Log the call for the transform() function
# transformed = transform()
# log_progress("Data transformation complete. Initiating Loading Process")
