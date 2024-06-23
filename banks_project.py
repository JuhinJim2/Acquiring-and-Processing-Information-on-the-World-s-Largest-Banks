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
    data = BeautifulSoup(html_page,'html_parser')

    tables = data.find_all('table')
    rows = tables[2].find_all('tr')

    count = 0

    for row in rows:
        if count < 10:
            col = row.find_all('td')
            if len(col) != 0:
                data_dict = {'Name': col[1].contents[1],
                             'MC_USD_Billion': col[2].contents[2]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index = True)
                count+=1
        else:
            break
    
    return df

# Log declared known values
log_progress("Preliminaries complete. Initating ETL process")

# Log the call of the extract() function
extracted_data = extract()
log_progress("Data extraction complete. Initiating Transformation process")

# Load the extracted data
df.to_csv(csv_path)


# #Log the call for the transform() function
# transformed = transform()
# log_progress("Data transformation complete. Initiating Loading Process")
