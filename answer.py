import os
import json
import re
import traceback
import subprocess
from dotenv import load_dotenv
import google.generativeai as genai
from DataScraping.csv2json import csv_to_json

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
MODEL = genai.GenerativeModel("gemini-1.5-flash")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRAPED_PATH = os.path.join(BASE_DIR, "scraped_data.json")
QUESTION_PATH = os.path.join(BASE_DIR, "question1.txt")
ANSWER_PROMPT_PATH = os.path.join(BASE_DIR, "answer_prompt.txt")
OUTPUT_CODE_PATH = os.path.join(BASE_DIR, "output_code.py")
FINAL_OUTPUT_PATH = os.path.join(BASE_DIR, "final_output.json")
FEEDBACK_PATH = os.path.join(BASE_DIR, "feedback_processing.txt")

def clean_markdown(code):
    return re.sub(r"^```(?:python)?\s*|```$", "", code.strip(), flags=re.MULTILINE)

def generate_initial_code(question_text: str):
    try:
        csv_to_json()
    except Exception as e:
        raise RuntimeError(f"CSV to JSON conversion failed: {e}")

    with open(SCRAPED_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(ANSWER_PROMPT_PATH, "r", encoding="utf-8") as f:
        template = f.read()

    full_prompt = template.format(data=json.dumps(data, indent=2), questions=question_text)
    response = MODEL.generate_content(full_prompt)
    return clean_markdown(response.text)


def generate_and_save_code(question_text: str):
    try:
        code = generate_initial_code(question_text)
        with open(OUTPUT_CODE_PATH, "w", encoding="utf-8") as f:
            f.write(code)
        return {"status": "success", "message": "Code saved to output_code.py"}
    except Exception as e:
        return {"status": "error", "message": f"Code generation failed: {e}"}

def revise_code(code, error_msg, output):
    with open(FEEDBACK_PATH, "r", encoding="utf-8") as f:
        feedback_template = f.read()

    feedback_prompt = feedback_template.format(code=code, status=error_msg, output=output)
    response = MODEL.generate_content(feedback_prompt)
    return clean_markdown(response.text)

def run_code_with_retries(code, retries=5):
    for attempt in range(retries):
        try:
            with open(OUTPUT_CODE_PATH, "w", encoding="utf-8") as f:
                f.write(code)

            result = subprocess.run(
                ["python", OUTPUT_CODE_PATH],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                timeout=60
            )

            # Debug logs for troubleshooting
            print(f"Attempt {attempt + 1}, returncode: {result.returncode}, stderr: {result.stderr.strip()}")

            if result.returncode != 0 or result.stderr.strip():
                output = f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
                error_msg = f"Attempt {attempt + 1} failed with non-zero exit or stderr."
                code = revise_code(code, error_msg, output)
                continue

            if not os.path.exists(FINAL_OUTPUT_PATH):
                output = f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}\nMissing output file."
                error_msg = f"Attempt {attempt + 1} failed: output file missing."
                code = revise_code(code, error_msg, output)
                continue

            with open(FINAL_OUTPUT_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Treat output containing error key as failure
            if isinstance(data, dict) and "error" in data:
                error_msg = f"Attempt {attempt + 1} failed: output file contains error message."
                code = revise_code(code, error_msg, json.dumps(data))
                continue

            return data

        except Exception as e:
            output = f"Exception: {str(e)}"
            error_msg = traceback.format_exc()
            code = revise_code(code, error_msg, output)

    return {"error": "All attempts failed", "last_code": code}

def run_code_and_save_answer():
    try:
        with open(OUTPUT_CODE_PATH, "r", encoding="utf-8") as f:
            code = f.read()

        result = run_code_with_retries(code)

        if isinstance(result, dict) and "error" in result:
            return {"status": "error", "message": result["error"], "last_code": result.get("last_code", "")}

        if isinstance(result, (dict, list)):
            return {"status": "success", "answer": result}

        return {"status": "error", "message": "Generated result is invalid"}

    except Exception as e:
        return {"status": "error", "message": f"Execution failed: {e}"}
