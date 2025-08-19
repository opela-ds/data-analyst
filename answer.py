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
        print("[DEBUG] Converting CSV to JSON...")
        csv_to_json()
    except Exception as e:
        print(f"[ERROR] CSV to JSON conversion failed: {e}")
        raise RuntimeError(f"CSV to JSON conversion failed: {e}")

    with open(SCRAPED_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(ANSWER_PROMPT_PATH, "r", encoding="utf-8") as f:
        template = f.read()

    full_prompt = template.format(data=json.dumps(data, indent=2), questions=question_text)
    print("[DEBUG] Sending prompt to Gemini model...")
    response = MODEL.generate_content(full_prompt)
    return clean_markdown(response.text)

def generate_and_save_code(question_text: str):
    try:
        code = generate_initial_code(question_text)
        with open(OUTPUT_CODE_PATH, "w", encoding="utf-8") as f:
            f.write(code)
        print(f"[DEBUG] Code successfully generated and saved to {OUTPUT_CODE_PATH}")
        return {"status": "success", "message": "Code saved to output_code.py"}
    except Exception as e:
        print(f"[ERROR] Code generation failed: {e}")
        return {"status": "error", "message": f"Code generation failed: {e}"}

def revise_code(code, error_msg, output):
    print("[DEBUG] Revising code due to error...")
    print(f"[DEBUG] Error Msg: {error_msg}")
    print(f"[DEBUG] Output:\n{output[:500]}...")  # print first 500 chars
    with open(FEEDBACK_PATH, "r", encoding="utf-8") as f:
        feedback_template = f.read()

    feedback_prompt = feedback_template.format(code=code, status=error_msg, output=output)
    response = MODEL.generate_content(feedback_prompt)
    return clean_markdown(response.text)

def run_code_with_retries(code, retries=5):
    last_stdout, last_stderr = "", ""
    for attempt in range(retries):
        print(f"\n[DEBUG] ===== Attempt {attempt+1}/{retries} =====")
        try:
            with open(OUTPUT_CODE_PATH, "w", encoding="utf-8") as f:
                f.write(code)

            print(f"[DEBUG] Running subprocess: python {OUTPUT_CODE_PATH}")
            result = subprocess.run(
                ["python", OUTPUT_CODE_PATH],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                timeout=60
            )

            last_stdout, last_stderr = result.stdout, result.stderr

            print(f"[DEBUG] Return code: {result.returncode}")
            print(f"[DEBUG] STDOUT:\n{result.stdout}")
            print(f"[DEBUG] STDERR:\n{result.stderr}")

            if result.returncode != 0 or result.stderr.strip():
                error_msg = f"Attempt {attempt+1} failed (non-zero exit or stderr)"
                code = revise_code(code, error_msg, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
                continue

            if not os.path.exists(FINAL_OUTPUT_PATH):
                error_msg = f"Attempt {attempt+1} failed: {FINAL_OUTPUT_PATH} not found"
                code = revise_code(code, error_msg, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
                continue

            with open(FINAL_OUTPUT_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict) and "error" in data:
                error_msg = f"Attempt {attempt+1} failed: output file contains error"
                code = revise_code(code, error_msg, json.dumps(data, indent=2))
                continue

            print("[DEBUG] Successfully generated final output.")
            return data

        except Exception as e:
            print(f"[ERROR] Exception in attempt {attempt+1}: {e}")
            traceback.print_exc()
            code = revise_code(code, f"Exception: {str(e)}", traceback.format_exc())

    print("[ERROR] All attempts failed.")
    return {
        "error": "All attempts failed",
        "last_code": code,
        "last_stdout": last_stdout,
        "last_stderr": last_stderr
    }

def run_code_and_save_answer():
    try:
        print("[DEBUG] Reading generated code from output_code.py...")
        with open(OUTPUT_CODE_PATH, "r", encoding="utf-8") as f:
            code = f.read()

        result = run_code_with_retries(code)

        if isinstance(result, dict) and "error" in result:
            print(f"[ERROR] Final result failed: {result}")
            return {"status": "error", "message": result["error"], "last_code": result.get("last_code", ""),
                    "stdout": result.get("last_stdout", ""), "stderr": result.get("last_stderr", "")}

        if isinstance(result, (dict, list)):
            print("[DEBUG] Final result succeeded.")
            return {"status": "success", "answer": result}

        print("[ERROR] Generated result is invalid.")
        return {"status": "error", "message": "Generated result is invalid"}

    except Exception as e:
        print(f"[ERROR] run_code_and_save_answer crashed: {e}")
        traceback.print_exc()
        return {"status": "error", "message": f"Execution failed: {e}"}
