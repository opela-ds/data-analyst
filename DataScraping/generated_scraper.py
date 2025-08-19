import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import io
import base64
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import json

def robust_float(value):
    try:
        value = re.sub(r'[^\d\.]', '', value)
        return float(value)
    except:
        return None

def robust_date(value):
    try:
        return pd.to_datetime(value, errors='coerce')
    except:
        return None

def find_best_table(url, keywords):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        print(f"Detected {len(tables)} tables.")

        best_table = None
        best_match_count = 0
        best_row_count = 0

        for table in tables:
            try:
                df = pd.read_html(io.StringIO(str(table)))[0]
                original_columns = df.columns.tolist()
                print("Original Columns:", original_columns)
                
                # Normalize column names
                df.columns = [re.sub(r'\[.*?\]|\(.*?\)', '', str(col)).strip().lower() for col in df.columns]

                match_count = sum(1 for col in df.columns if any(keyword in col for keyword in keywords))

                if match_count >= 2:
                    if match_count > best_match_count or (match_count == best_match_count and len(df) > best_row_count):
                        best_match_count = match_count
                        best_row_count = len(df)
                        best_table = df
                        best_original_columns = original_columns

            except Exception as e:
                print(f"Error processing table: {e}")
                continue

        if best_table is not None:
            print("Best Table Columns:", best_table.columns.tolist())
            return best_table, best_original_columns
        else:
            print("No suitable table found.")
            return None, None

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None, None
    except Exception as e:
        print(f"General error: {e}")
        return None, None

def clean_dataframe(df, original_columns):
    try:
        # Data Cleaning
        for col in df.columns:
            try:
                df[col] = df[col].astype(str).str.replace(r'\[.*?\]|\(.*?\)', '', regex=True).str.strip()
            except:
                pass
            
            # Try converting to numeric or date
            try:
                df[col] = df[col].apply(robust_float)
            except:
                try:
                    df[col] = df[col].apply(robust_date)
                except:
                    pass

        # Remove empty rows and columns
        df = df.dropna(axis=1, how='all')
        df = df.dropna(axis=0, how='all')

        # Remove rows with mostly empty values (more than half the columns are NaN)
        threshold = len(df.columns) // 2
        df = df.dropna(axis=0, thresh=threshold)
        df = df.reset_index(drop=True)

        # Ensure at least 5 meaningful rows
        if len(df) < 5:
            print("Not enough meaningful rows after cleaning.")
            return None

        return df

    except Exception as e:
        print(f"Error cleaning dataframe: {e}")
        return None

def analyze_data(df):
    try:
        # 1. How many $2 bn movies were released before 2000?
        billion_movies = df[df['gross'] >= 2000000000]
        before_2000 = billion_movies[billion_movies['year'] < 2000]
        num_2bn_before_2000 = len(before_2000)

        # 2. Which is the earliest film that grossed over $1.5 bn?
        over_1_5bn = df[df['gross'] >= 1500000000].sort_values(by='year').iloc[0]['title']

        # 3. What's the correlation between the Rank and Peak?
        correlation = df['rank'].corr(df['peak'])

        # 4. Draw a scatterplot of Rank and Peak along with a dotted red regression line through it.
        x = df['rank']
        y = df['peak']
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        line = slope * x + intercept

        plt.figure(figsize=(8, 6))
        plt.scatter(x, y, label='Data')
        plt.plot(x, line, 'r--', label=f'Regression Line (R={r_value:.2f})')
        plt.xlabel('Rank')
        plt.ylabel('Peak')
        plt.title('Scatterplot of Rank vs Peak')
        plt.legend()
        plt.grid(True)

        # Save the plot to a BytesIO object
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        img_data = buf.read()
        plt.close()

        # Encode the image data to base64
        img_base64 = base64.b64encode(img_data).decode('utf-8')
        image_uri = f"data:image/png;base64,{img_base64}"
        
        return num_2bn_before_2000, over_1_5bn, correlation, image_uri

    except Exception as e:
        print(f"Error during analysis: {e}")
        return None, None, None, None

def main():
    url = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"
    keywords = ["rank", "title", "name", "film", "year", "gross", "revenue", "peak"]
    
    best_table, best_original_columns = find_best_table(url, keywords)

    if best_table is not None:
        cleaned_df = clean_dataframe(best_table, best_original_columns)

        if cleaned_df is not None:
            try:
                cleaned_df.to_csv('scraped_data.csv', index=False)
                print("Data saved to scraped_data.csv")

                # Analyze the data
                cleaned_df.columns = [col.lower() for col in cleaned_df.columns]  # Ensure lowercase columns
                num_2bn_before_2000, over_1_5bn, correlation, image_uri = analyze_data(cleaned_df)

                if all(v is not None for v in [num_2bn_before_2000, over_1_5bn, correlation, image_uri]):
                    answers = [
                        str(num_2bn_before_2000),
                        str(over_1_5bn),
                        str(correlation),
                        image_uri
                    ]
                    print(json.dumps(answers))
                else:
                    print("Could not calculate answers.")
            except Exception as e:
                print(f"Error saving to CSV or analyzing: {e}")
        else:
            print("Data cleaning failed.")
    else:
        print("No suitable table found for scraping.")

if __name__ == "__main__":
    main()