import os
import json
import re
import traceback
import subprocess
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
MODEL = genai.GenerativeModel("gemini-1.5-flash")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRAPED_PATH = os.path.join(BASE_DIR, "DataScraping", "scraped_data.json")
QUESTION_PATH = os.path.join(BASE_DIR, "question1.txt")
ANSWER_PROMPT_PATH = os.path.join(BASE_DIR, "answer_prompt.txt")
OUTPUT_CODE_PATH = os.path.join(BASE_DIR, "output_code.py")
FINAL_OUTPUT_PATH = os.path.join(BASE_DIR, "output_answer.json")
FEEDBACK_PATH = os.path.join(BASE_DIR, "feedback_processing.txt")

def clean_markdown(code):
    return re.sub(r"^```(?:python)?\s*|```$", "", code.strip(), flags=re.MULTILINE)

def generate_initial_code():
    with open(SCRAPED_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(QUESTION_PATH, "r", encoding="utf-8") as f:
        questions = f.read()

    with open(ANSWER_PROMPT_PATH, "r", encoding="utf-8") as f:
        template = f.read()

    full_prompt = template.format(data=json.dumps(data, indent=2), questions=questions)
    response = MODEL.generate_content(full_prompt)
    return clean_markdown(response.text)

def revise_code(code, error_msg, output):
    with open(FEEDBACK_PATH, "r", encoding="utf-8") as f:
        feedback_template = f.read()

    feedback_prompt = feedback_template.format(code=code, status=error_msg, output=output)
    response = MODEL.generate_content(feedback_prompt)
    return clean_markdown(response.text)

def run_code_with_retries(code, retries=3):
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

            if os.path.exists(FINAL_OUTPUT_PATH):
                with open(FINAL_OUTPUT_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)

            output = f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            error_msg = f"Attempt {attempt + 1} failed:\n{traceback.format_exc()}"
            code = revise_code(code, error_msg, output)

        except Exception as e:
            output = f"Exception: {str(e)}"
            error_msg = traceback.format_exc()
            code = revise_code(code, error_msg, output)

    return {"error": "All attempts failed", "last_code": code}

def generate_and_save_code():
    try:
        code = generate_initial_code()
        with open(OUTPUT_CODE_PATH, "w", encoding="utf-8") as f:
            f.write(code)
        return {"status": "success", "message": "Code saved to output_code.py"}
    except Exception as e:
        return {"status": "error", "message": f"Code generation failed: {e}"}

def run_code_and_save_answer():
    try:
        with open(OUTPUT_CODE_PATH, "r", encoding="utf-8") as f:
            code = f.read()

        result = run_code_with_retries(code)

        if isinstance(result, (dict, list)):
            with open(FINAL_OUTPUT_PATH, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            return {"status": "success", "message": "Answer saved to output_answer.json"}

        return {"status": "error", "message": "Generated result is invalid"}

    except Exception as e:
        return {"status": "error", "message": f"Execution failed: {e}"}
