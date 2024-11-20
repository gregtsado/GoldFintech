import requests
import json
import pandas as pd
from dotenv import load_dotenv
import os

apikey = os.getenv('api_key')

def fetch_data():
    url= f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=MSFT&apikey={apikey}'
    r = requests.get(url)
    data = r.json()
    
    data = data['Weekly Time Series']

# Initialize a list to store rows
    rows = []

# Loop through each date and corresponding data in the Weekly Time Series
    for date, values in data.items():
        # Create a dictionary for each row
        rows.append({
            'week': date,
            'open': values['1. open'],
            'high': values['2. high'],
            'low': values['3. low'],
            'close': values['4. close'],
            'volume': values['5. volume']
        })

    # Convert list of dictionaries into a DataFrame
    df = pd.DataFrame(rows)

    # Display the DataFrame
    df.to_csv('msftstock.csv', index=False)
    
    print(f'{len(df)} rows successfully extracted')
    

