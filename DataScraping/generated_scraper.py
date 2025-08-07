import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import re
import numpy as np

def clean_value(value):
    """
    Cleans a string value by removing citations and converting to numeric if possible.
    """
    value = re.sub(r"\[.*?\]", "", value)
    value = value.replace(",", "")
    try:
        return float(value)
    except ValueError:
        return value.strip()

def scrape_highest_grossing_films(url):
    """
    Scrapes data from the Wikipedia page and returns a pandas DataFrame.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'wikitable'})

        if not table:
            print("Error: Could not find the table.")
            return None

        header_row = table.find_all('th')
        header = [th.text.strip() for th in header_row]

        data = []
        rows = table.find_all('tr')[1:]  # Skip the header row

        for row in rows:
            cells = row.find_all('td')
            if cells:
                row_data = [clean_value(cell.text) for cell in cells]

                # Handle inconsistent column counts - fill with NaN
                if len(row_data) < len(header):
                    row_data.extend([''] * (len(header) - len(row_data)))
                elif len(row_data) > len(header):
                    row_data = row_data[:len(header)]  # Trim extra columns

                data.append(row_data)

        df = pd.DataFrame(data, columns=header)

        # Rename columns to be more standard
        df = df.rename(columns={
            "Rank": "Rank",
            "Title": "Title",
            "Worldwide gross": "Worldwide_Gross",
            "Year": "Year",
            "Ref.": "Ref" #Leave this for now. Handle later if needed.
        })

        # Ensure Worldwide Gross is numeric and handle missing values
        try:
            df['Worldwide_Gross'] = pd.to_numeric(df['Worldwide_Gross'], errors='coerce')
        except KeyError:
            print("Error: 'Worldwide Gross' column not found. Check the table structure.")
            return None
        df['Worldwide_Gross'] = df['Worldwide_Gross'].fillna(0)

        try:
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
        except KeyError:
            print("Error: 'Year' column not found. Check the table structure.")
            return None
        df['Year'] = df['Year'].fillna(0)

        try:
            df['Rank'] = pd.to_numeric(df['Rank'], errors='coerce')
        except KeyError:
            print("Error: 'Rank' column not found. Check the table structure.")
            return None
        df['Rank'] = df['Rank'].fillna(0)
        
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error: Could not retrieve the webpage. {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


# URL of the Wikipedia page
url = 'https://en.wikipedia.org/wiki/List_of_highest-grossing_films'

# Scrape the data
df = scrape_highest_grossing_films(url)

# Save to CSV if the dataframe exists
if df is not None:
    df.to_csv('scraped_data.csv', index=False)
    print("Data saved to scraped_data.csv")
else:
    print("Data scraping failed. Check error messages.")