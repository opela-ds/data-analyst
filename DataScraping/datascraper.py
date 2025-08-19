import subprocess
import os
import pandas as pd
import traceback
import google.generativeai as genai
import re
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))

CSV_PATH = os.path.join(PROJECT_ROOT, "scraped_data.csv")

import os

def save_uploaded_question(uploaded_file, scraped_json_path="scraped_data.json", prefix="question"):
    """
    Save the uploaded question file into the same directory where scraped_data.json is located.
    Creates sequentially numbered question files (question1.txt, question2.txt, ...).
    """
    folder = os.path.dirname(scraped_json_path)
    if not folder:
        folder = "."  # current directory
    
    os.makedirs(folder, exist_ok=True)

    existing = [
        f for f in os.listdir(folder)
        if f.startswith(prefix) and f.endswith(".txt")
    ]
    numbers = []
    for f in existing:
        try:
            num = int(f[len(prefix):-4])
            numbers.append(num)
        except ValueError:
            continue

    next_num = max(numbers) + 1 if numbers else 1
    filename = f"{prefix}{next_num}.txt"
    filepath = os.path.join(folder, filename)

    with open(filepath, "wb") as f:
        f.write(uploaded_file.file.read())

    print(f"[save_uploaded_question] Saved: {filepath}")
    return filepath


def debug_generated_code(scraper_path):
    """Debug the generated scraper code to identify common issues"""
    print("[debug] Analyzing generated scraper code...")
    
    try:
        with open(scraper_path, 'r', encoding='utf-8') as f:
            code = f.read()
        
        # Check for common problematic patterns
        issues = []
        warnings = []
        
        # Check for hardcoded column assumptions
        hardcoded_patterns = [
            r"dropna\(subset=\[.*?'(Gross|Year|Rank|Title)'.*?\]",
            r"df\['(Gross|Year|Rank|Title)'\]",
            r"subset=\[.*?'(Gross|Year|Rank|Title)'.*?\]",
            r"\.drop\(.*?'(Gross|Year|Rank|Title)'.*?\)"
        ]
        
        for pattern in hardcoded_patterns:
            matches = re.findall(pattern, code, re.IGNORECASE)
            if matches:
                issues.append(f"Found hardcoded column assumptions: {set(matches)}")
        
        # Check for column existence checks
        if 'in df.columns' not in code and 'columns' in code:
            issues.append("No column existence checks found - code may fail on missing columns")
        
        # Check for proper error handling around DataFrame operations
        df_operations = ['pd.DataFrame', 'dropna', 'to_csv', 'drop_duplicates']
        for op in df_operations:
            if op in code:
                # Look for try-catch around this operation
                lines = code.split('\n')
                for i, line in enumerate(lines):
                    if op in line:
                        # Check if there's a try block within reasonable distance
                        try_found = any('try:' in lines[j] for j in range(max(0, i-10), min(len(lines), i+3)))
                        if not try_found:
                            warnings.append(f"Operation '{op}' found without try-catch protection")
                        break
        
        # Check for DataFrame creation issues
        if 'pd.DataFrame(' in code and 'columns=' in code:
            if not re.search(r'len\(.*?columns.*?\)', code) and not re.search(r'len\(.*?headers.*?\)', code):
                issues.append("DataFrame creation without length validation may cause column mismatch")
        
        # Check for proper table parsing
        if 'find_all' in code and 'table' in code:
            if 'normalize' not in code and 'consistent' not in code:
                warnings.append("Table parsing may not handle inconsistent row structures")
        
        # Look for specific error patterns from the logs
        error_indicators = [
            'columns passed, passed data had',
            'KeyError:',
            'dropna(subset='
        ]
        
        # Check if code handles the common error cases
        if any(indicator in code for indicator in ['KeyError', 'columns passed']):
            warnings.append("Code contains error-prone patterns")
        
        # Print results
        if issues:
            print("[debug] CRITICAL issues detected:")
            for issue in issues:
                print(f"  - {issue}")
        
        if warnings:
            print("[debug] Potential issues detected:")
            for warning in warnings:
                print(f"  - {warning}")
        
        if not issues and not warnings:
            print("[debug] No obvious issues detected in generated code")
        
        # Analyze the problematic data you showed
        print("\n[debug] Based on your data sample, the scraper is likely:")
        print("  - Concatenating column headers as data (year19151916... pattern)")
        print("  - Not properly separating table headers from data")
        print("  - Creating malformed CSV structure")
        
    except Exception as e:
        print(f"[debug] Error analyzing code: {e}")

def analyze_csv_structure(csv_path=CSV_PATH):
    """Analyze the structure of the generated CSV to identify issues"""
    print("[analyze] Analyzing CSV structure...")
    
    if not os.path.exists(csv_path):
        print("[analyze] CSV file not found")
        return
    
    try:
        # Read raw file content first
        with open(csv_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()[:1000]  # First 1000 chars
        
        print(f"[analyze] Raw CSV content preview:\n{raw_content}")
        
        # Try to read as CSV
        df = pd.read_csv(csv_path)
        print(f"[analyze] CSV shape: {df.shape}")
        print(f"[analyze] Columns: {list(df.columns)}")
        
        # Check for common structural issues
        issues = []
        
        # Check column names
        for col in df.columns:
            if len(str(col)) > 100:
                issues.append(f"Extremely long column name detected: {str(col)[:50]}...")
            if any(char in str(col) for char in ['19', '20']) and len(str(col)) > 20:
                issues.append(f"Column name contains years/dates suggesting header concatenation: {str(col)[:50]}...")
        
        # Check data patterns
        if df.shape[0] > 0:
            first_row = df.iloc[0]
            for col, value in first_row.items():
                if pd.isna(value) or str(value).strip() == '':
                    continue
                if str(value) == '0' or str(value) == '0.0':
                    issues.append(f"Many zero values detected - possible parsing issue")
                    break
        
        if issues:
            print("[analyze] Structural issues found:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("[analyze] CSV structure appears normal")
            
    except Exception as e:
        print(f"[analyze] Error analyzing CSV: {e}")

def postprocess_csv(csv_path=CSV_PATH):
    print(f"[postprocess_csv] Checking CSV at: {csv_path}")
    if not os.path.exists(csv_path):
        print(f"[postprocess_csv] CSV file not found at {csv_path}")
        return False

    try:
        # First, analyze the problematic structure
        analyze_csv_structure(csv_path)
        
        df = pd.read_csv(csv_path)
        print(f"[postprocess_csv] CSV loaded, shape: {df.shape}")

        # Check for severely malformed data (like your example)
        if df.shape[1] <= 2 and df.shape[0] <= 10:
            # Check if columns contain concatenated data
            for col in df.columns:
                col_str = str(col)
                if len(col_str) > 50 or any(pattern in col_str.lower() for pattern in ['rank', 'year', 'title', 'gross']):
                    print(f"[postprocess_csv] WARNING: Malformed column detected: {col_str[:100]}...")
                    print("[postprocess_csv] This suggests the scraper concatenated headers instead of parsing table structure")
                    return False

        # Remove completely empty columns and rows
        df = df.dropna(axis=1, how='all')
        df = df.loc[:, (df != '').any(axis=0)]
        df.columns = df.columns.str.strip()

        # Clean string columns
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.strip()

        # Convert numeric columns more carefully
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            # Skip if column name suggests it should be text (title, name, etc.)
            if any(keyword in col.lower() for keyword in ['title', 'name', 'film', 'movie', 'director', 'actor', 'description']):
                continue
                
            # Try to convert to numeric, but be more conservative
            cleaned_values = df[col].astype(str).str.replace(r'[^\d\.\-]', '', regex=True)
            converted = pd.to_numeric(cleaned_values, errors='coerce')
            
            # Only convert if more than 70% of values are numeric and it's not already text-heavy
            if converted.notna().sum() / len(df) > 0.7:
                df[col] = converted

        # Date parsing - be much more selective
        for col in df.columns:
            # Only try date parsing on columns that likely contain dates
            if any(keyword in col.lower() for keyword in ['date', 'release', 'premiere', 'published']):
                try:
                    parsed_dates = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
                    # Only convert if most values are valid dates AND they look like real dates (not just numbers)
                    if (parsed_dates.notna().sum() / len(df) > 0.8 and 
                        not df[col].astype(str).str.match(r'^\d+$').all()):  # Not just plain numbers
                        df[col] = parsed_dates
                except Exception as e:
                    print(f"[postprocess_csv] Date parsing skipped for column '{col}': {e}")

        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                pass
            else:
                df[col] = df[col].fillna("")

        df = df.dropna(how='all')
        
        text_cols = df.select_dtypes(include=['object']).columns
        if len(text_cols) > 0:
            df = df[df[text_cols].apply(lambda x: x.astype(str).str.strip().ne('').any(), axis=1)]

        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"[postprocess_csv] CSV postprocessing complete and saved")
        return True
        
    except Exception as e:
        print(f"[postprocess_csv] Exception in postprocess_csv: {e}")
        traceback.print_exc()
        return False

def is_csv_valid():
    errors = []
    csv_path = CSV_PATH
    print(f"[is_csv_valid] Validating CSV at: {csv_path}")
    try:
        if not os.path.exists(csv_path):
            errors.append("CSV file not found")
            print("[is_csv_valid] CSV file not found")
            return False, errors
        
        df = pd.read_csv(csv_path)
        print(f"[is_csv_valid] CSV loaded, shape: {df.shape}")

        if df.empty or df.shape[0] < 5:
            errors.append("CSV has fewer than 5 rows or is empty")
            print("[is_csv_valid] CSV too small or empty")

        if any(col.strip() == '' for col in df.columns):
            errors.append("CSV contains empty column names")
            print("[is_csv_valid] Empty column names detected")

        # Check for malformed column names (like your concatenated headers)
        for col in df.columns:
            col_str = str(col)
            if len(col_str) > 100:
                errors.append(f"Column name too long, suggests malformed parsing: {col_str[:50]}...")
                print(f"[is_csv_valid] Long column name detected: {col_str[:50]}...")

        text_cols = [c for c in df.columns if df[c].dtype == object]
        if not text_cols:
            errors.append("No text columns found")
            print("[is_csv_valid] No text columns found")
        else:
            try:
                all_text_empty = True
                for col in text_cols:
                    non_empty_count = df[col].astype(str).str.strip().replace('', None).dropna().shape[0]
                    if non_empty_count > 0:
                        all_text_empty = False
                        break
                
                if all_text_empty:
                    errors.append("All text columns are empty or contain only whitespace")
                    print("[is_csv_valid] All text columns empty or blanks")
            except Exception as e:
                errors.append(f"Error validating text columns: {e}")
                print(f"[is_csv_valid] Error checking text columns: {e}")

        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if not numeric_cols:
            errors.append("No numeric columns found")
            print("[is_csv_valid] No numeric columns found")
        else:
            try:
                valid_numeric = False
                for col in numeric_cols:
                    meaningful_values = df[col].dropna().replace(0, pd.NA).dropna().shape[0]
                    if meaningful_values > 0:
                        valid_numeric = True
                        break
                
                if not valid_numeric:
                    errors.append("All numeric columns contain only zeros, nulls, or are empty")
                    print("[is_csv_valid] Numeric columns contain no meaningful data")
            except Exception as e:
                errors.append(f"Error validating numeric columns: {e}")
                print(f"[is_csv_valid] Error checking numeric columns: {e}")

        if errors:
            print(f"[is_csv_valid] Validation errors: {errors}")
        return (len(errors) == 0), errors

    except Exception as e:
        errors.append(f"Exception during validation: {e}")
        print(f"[is_csv_valid] Exception during validation: {e}")
        traceback.print_exc()
        return False, errors

def task_breakdown(file_path: str):
    print(f"[task_breakdown] Starting task breakdown with question file: {file_path}")

    with open("DataScraping/task_breakdown.txt", "r", encoding="utf-8") as p:
        base_prompt = p.read()

    with open(file_path, "r", encoding="utf-8") as q:
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
        print(f"--- Attempt {attempts + 1}/{max_attempts} ---")

        try:
            response = model.generate_content(full_prompt)
            code_blocks = re.findall(r"```(?:python)?\n(.*?)```", response.text, re.DOTALL)
            code = code_blocks[0].strip() if code_blocks else response.text.strip()

            scraper_path = os.path.join("DataScraping", "generated_scraper.py")
            with open(scraper_path, "w", encoding="utf-8") as f:
                f.write(code)
            print(f"[task_breakdown] Written scraper code to {scraper_path}")
            
            # Debug the generated code before running
            debug_generated_code(scraper_path)

        except Exception as e:
            print(f"[task_breakdown] Error generating code: {e}")
            attempts += 1
            continue

        try:
            result = subprocess.run(
                ["python", scraper_path],
                capture_output=True,
                text=True,
                timeout=60,
                cwd=PROJECT_ROOT
            )
            output = result.stdout + "\n" + result.stderr
            print(f"[task_breakdown] Scraper stdout:\n{result.stdout}")
            print(f"[task_breakdown] Scraper stderr:\n{result.stderr}")
        
        except subprocess.TimeoutExpired:
            output = "Execution timed out after 60 seconds"
            print(f"[task_breakdown] {output}")
        except Exception:
            output = f"Execution crashed:\n{traceback.format_exc()}"
            print(f"[task_breakdown] Exception running scraper:\n{output}")

        if os.path.exists(CSV_PATH):
            size = os.path.getsize(CSV_PATH)
            print(f"[task_breakdown] CSV file size after scraping: {size} bytes")
            if size > 0:
                try:
                    with open(CSV_PATH, "r", encoding="utf-8") as f:
                        snippet = f.read(500)
                        print(f"[task_breakdown] CSV file content preview:\n{snippet}")
                except Exception as e:
                    print(f"[task_breakdown] Could not preview CSV: {e}")
            else:
                print("[task_breakdown] CSV file is empty")
        else:
            print("[task_breakdown] CSV file does not exist after scraping")

        postprocess_success = postprocess_csv()
        if not postprocess_success:
            print("[task_breakdown] Post-processing failed - likely malformed data")

        valid, validation_errors = is_csv_valid()
        if valid:
            print("[task_breakdown] CSV is valid. Breaking loop.")
            break
        else:
            print(f"[task_breakdown] CSV validation failed: {validation_errors}")

        if os.path.exists(CSV_PATH):
            try:
                df = pd.read_csv(CSV_PATH)
                csv_preview = df.head().to_csv(index=False)
                print(f"[task_breakdown] CSV preview:\n{csv_preview}")
            except Exception as e:
                csv_preview = f"Failed to preview CSV: {e}"
                print(f"[task_breakdown] {csv_preview}")
        else:
            csv_preview = "CSV file does not exist"
            print("[task_breakdown] CSV preview: File does not exist")

        # Enhanced error context for feedback
        error_summary = "; ".join(validation_errors) if validation_errors else "Unknown validation errors"
        
        full_prompt = feedback_template.format(
            previous_code=code,
            output=output,
            csv_status="was created" if os.path.exists(CSV_PATH) else "was NOT created",
            csv_preview=csv_preview,
            errors=error_summary
        )

        attempts += 1

    if attempts >= max_attempts:
        print(f"[task_breakdown] Maximum attempts ({max_attempts}) reached. Task breakdown failed.")
        print("[task_breakdown] Common issues identified:")
        print("  - Table structure parsing problems")
        print("  - Header/data row misalignment")
        print("  - Hardcoded column assumptions")
        print("  - Insufficient error handling")
    else:
        print("[task_breakdown] Task breakdown completed successfully.")
    
    return code
