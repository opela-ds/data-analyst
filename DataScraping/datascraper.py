import subprocess
import os
import pandas as pd
import traceback
import google.generativeai as genai
import re

def is_csv_valid():
    try:
        csv_path = "DataScraping/scraped_data.csv"
        if not os.path.exists(csv_path):
            return False
        df = pd.read_csv(csv_path)
        if df.empty or df.shape[0] < 5:
            return False
        numeric_cols = [col for col in df.columns if df[col].dtype in ['float64', 'int64']]
        if not numeric_cols:
            return False
        if "Year" in df.columns and df["Year"].max() > 2100:
            return False
        return True
    except Exception:
        return False

def task_breakdown(_: str = ""):
    with open("DataScraping/task_breakdown.txt", "r", encoding="utf-8") as p:
        base_prompt = p.read()

    with open("question1.txt", "r", encoding="utf-8") as q:
        question = q.read()

    with open("DataScraping/feedback_scraper.txt", "r", encoding="utf-8") as f:
        feedback_template = f.read()

    full_prompt = f"{base_prompt}\n\n{question}"

    model = genai.GenerativeModel(model_name="gemini-2.0-flash")

    attempts = 0
    max_attempts = 5
    code = ""
    output = ""

    while attempts < max_attempts:
        print(f"--- Attempt {attempts} ---")

        response = model.generate_content(full_prompt)
        code_blocks = re.findall(r"```(?:python)?\n(.*?)```", response.text, re.DOTALL)
        code = code_blocks[0].strip() if code_blocks else response.text.strip()

        scraper_path = "DataScraping/generated_scraper.py"
        with open(scraper_path, "w", encoding="utf-8") as f:
            f.write(code)

        try:
            result = subprocess.run(["python", scraper_path], capture_output=True, text=True, timeout=60)
            output = result.stdout + "\n" + result.stderr
        except Exception as e:
            output = f"Execution crashed:\n{traceback.format_exc()}"

        if is_csv_valid():
            print("✅ CSV is valid. Breaking loop.")
            break

        # Defensive preview
        if os.path.exists("DataScraping/scraped_data.csv"):
            try:
                df = pd.read_csv("DataScraping/scraped_data.csv")
                csv_preview = df.head().to_csv(index=False)
            except Exception:
                csv_preview = "Failed to preview CSV (possibly empty or malformed)"
        else:
            csv_preview = "N/A"

        full_prompt = feedback_template.format(
            previous_code=code,
            output=output,
            csv_status="was created" if os.path.exists("DataScraping/scraped_data.csv") else "was NOT created",
            csv_preview=csv_preview,
        )

        attempts += 1

    return code
