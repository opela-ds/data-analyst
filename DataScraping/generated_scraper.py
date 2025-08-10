import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import io
import base64
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import linregress

def extract_tables(url):
    """
    Extracts all tables from a given URL.

    Args:
        url (str): The URL to scrape.

    Returns:
        list: A list of pandas DataFrames, each representing a table.
              Returns an empty list if no tables are found or if an error occurs.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        return [pd.read_html(str(table), encoding='utf-8')[0] for table in tables]
    except requests.exceptions.RequestException as e:
        print(f"Error during requests to {url}: {e}")
        return []
    except Exception as e:
        print(f"Error parsing tables from {url}: {e}")
        return []

def is_relevant_column(column_name):
    """
    Checks if a column name is relevant based on semantic matching.

    Args:
        column_name (str): The column name to check.

    Returns:
        bool: True if the column is relevant, False otherwise.
    """
    relevant_keywords = ["rank", "title", "gross", "year", "value", "country", "city", "company", "peak", "worldwide", "domestic", "budget", "position"]
    column_name = column_name.lower()
    for keyword in relevant_keywords:
        if keyword in column_name:
            return True
    return False

def clean_dataframe(df):
    """
    Cleans a pandas DataFrame by normalizing data, removing units/footnotes,
    filling missing values, and removing irrelevant rows/columns.

    Args:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: The cleaned DataFrame.  Returns None if cleaning fails or if
                      the DataFrame does not have enough meaningful data after cleaning.
    """

    try:
        # Remove empty columns
        df = df.dropna(axis=1, how='all')

        # Remove columns with entirely null or empty string values.
        df = df.loc[:, (df.isnull().sum() < df.shape[0]) & ((df != '').sum() > 0)]

        # Remove URLs
        df = df.replace(r'http\S+', '', regex=True).replace(r'www\S+', '', regex=True)


        # Clean column names
        new_columns = []
        for col in df.columns:
            # Convert to string
            col = str(col)
            # Remove parentheses and brackets and content within them
            col = re.sub(r'\(.*?\)|\[.*?\]', '', col)
            # Remove special characters and extra spaces
            col = re.sub(r'[^a-zA-Z0-9\s]', '', col)
            col = col.strip()
            new_columns.append(col)
        df.columns = new_columns

        # Identify relevant columns dynamically
        relevant_columns = [col for col in df.columns if is_relevant_column(col)]
        if not relevant_columns:
            print("No relevant columns found in the table.")
            return None

        df = df[relevant_columns]

        # Clean the data in each column
        for col in df.columns:
            # Convert to string type for cleaning
            df[col] = df[col].astype(str)

            # Remove footnotes and references
            df[col] = df[col].str.replace(r'\[.*?\]', '', regex=True)
            df[col] = df[col].str.replace(r'\<.*?\>', '', regex=True)

            # Remove currency symbols and commas from numeric columns, then convert to numeric
            if 'gross' in col.lower() or 'value' in col.lower() or 'rank' in col.lower() or 'worldwide' in col.lower() or 'domestic' in col.lower() or 'budget' in col.lower():
                df[col] = df[col].str.replace(r'[$,]', '', regex=True)
                df[col] = df[col].str.replace(r'\(.*?\)','', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Clean whitespace
            df[col] = df[col].str.strip()

            # Handle year column
            if 'year' in col.lower():
                df[col] = df[col].str.extract(r'(\d{4})', expand=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric

            # Fill missing values
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(np.nan)  # Use NaN for numeric
            else:
                df[col] = df[col].fillna('')  # Use empty string for text

        # Remove empty rows
        df = df.dropna(how='all')

        # Ensure at least 5 meaningful data rows after cleaning
        if len(df) < 5:
            print("Not enough meaningful data rows after cleaning.")
            return None

        return df

    except Exception as e:
        print(f"Error cleaning DataFrame: {e}")
        return None

def find_and_clean_tables(url):
    """
    Finds, cleans, and saves relevant tables from a given URL.

    Args:
        url (str): The URL to scrape.

    Returns:
        list: A list of cleaned pandas DataFrames.
    """
    tables = extract_tables(url)
    cleaned_tables = []

    if not tables:
        print("No tables found on the page.")
        return cleaned_tables

    for i, table in enumerate(tables):
        cleaned_table = clean_dataframe(table)
        if cleaned_table is not None:
            cleaned_tables.append(cleaned_table)

    return cleaned_tables

def analyze_dataframes(dataframes):
    """
    Analyzes the cleaned dataframes and extracts the required information.

    Args:
        dataframes (list): A list of cleaned pandas DataFrames.

    Returns:
        list: A list containing the answers to the questions, formatted as strings.
    """

    if not dataframes:
        return ["No relevant data found."]

    combined_df = pd.concat(dataframes, ignore_index=True)

    # Filter for columns that are likely gross and year. Be flexible.
    gross_columns = [col for col in combined_df.columns if ('gross' in col.lower() or 'worldwide' in col.lower() or 'domestic' in col.lower() or 'value' in col.lower()) and pd.api.types.is_numeric_dtype(combined_df[col])]
    year_columns = [col for col in combined_df.columns if 'year' in col.lower() and pd.api.types.is_numeric_dtype(combined_df[col])]
    rank_columns = [col for col in combined_df.columns if ('rank' in col.lower() or 'position' in col.lower()) and pd.api.types.is_numeric_dtype(combined_df[col])]
    peak_columns = [col for col in combined_df.columns if 'peak' in col.lower() and pd.api.types.is_numeric_dtype(combined_df[col])]
    title_columns = [col for col in combined_df.columns if 'title' in col.lower()]

    if not (gross_columns and year_columns and rank_columns and peak_columns and title_columns):
        return ["Could not find all required columns (gross, year, rank, peak, title)."]

    gross_column = gross_columns[0]
    year_column = year_columns[0]
    rank_column = rank_columns[0]
    peak_column = peak_columns[0]
    title_column = title_columns[0]

    # 1. How many $2 bn movies were released before 2000?
    two_bn_movies_before_2000 = combined_df[(combined_df[gross_column] >= 2000000000) & (combined_df[year_column] < 2000)]
    num_two_bn_movies_before_2000 = len(two_bn_movies_before_2000)

    # 2. Which is the earliest film that grossed over $1.5 bn?
    films_over_1_5_bn = combined_df[combined_df[gross_column] > 1500000000].sort_values(by=year_column)
    earliest_film_over_1_5_bn = films_over_1_5_bn.iloc[0][title_column] if not films_over_1_5_bn.empty else "Not found"

    # 3. What's the correlation between the Rank and Peak?
    correlation = combined_df[rank_column].corr(combined_df[peak_column])

    # 4. Draw a scatterplot of Rank and Peak along with a dotted red regression line through it.
    plt.figure(figsize=(8, 6))
    plt.scatter(combined_df[rank_column], combined_df[peak_column], label="Data Points")

    # Calculate linear regression
    rank = combined_df[rank_column].dropna()
    peak = combined_df[peak_column].dropna()
    if not rank.empty and not peak.empty and len(rank) == len(peak):
        slope, intercept, r_value, p_value, std_err = linregress(rank, peak)
        plt.plot(rank, intercept + slope * rank, 'r--', label=f'Regression Line (R={r_value:.2f})')
    else:
        print("Not enough data to calculate regression line")

    plt.xlabel("Rank")
    plt.ylabel("Peak")
    plt.title("Scatterplot of Rank vs. Peak with Regression Line")
    plt.legend()

    # Convert plot to base64 encoded data URI
    img_buf = io.BytesIO()
    plt.savefig(img_buf, format='png')
    img_buf.seek(0)
    img_data = base64.b64encode(img_buf.read()).decode('utf-8')
    image_uri = f"data:image/png;base64,{img_data}"

    return [
        f"{num_two_bn_movies_before_2000}",
        f"{earliest_film_over_1_5_bn}",
        f"{correlation:.2f}",
        image_uri
    ]


# Main execution
url = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"
cleaned_dataframes = find_and_clean_tables(url)

if cleaned_dataframes:
    try:
        # Attempt to concatenate all cleaned dataframes
        combined_df = pd.concat(cleaned_dataframes, ignore_index=True)
        combined_df.to_csv('scraped_data.csv', index=False)
        print("Data saved to scraped_data.csv")

        # Analyze and print results
        results = analyze_dataframes(cleaned_dataframes)
        import json
        print(json.dumps(results))

    except Exception as e:
        print(f"Error concatenating and saving data: {e}")
else:
    print("No suitable data found to save.")