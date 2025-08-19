import os
import uvicorn
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json

from DataScraping.datascraper import task_breakdown
import answer
from DataScraping.csv2json import csv_to_json  # 🔹 import csv2json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRAPED_PATH = os.path.join(BASE_DIR, "DataScraping", "scraped_data.json")
QUESTION_PATH = os.path.join(BASE_DIR, "question1.txt")
FINAL_OUTPUT_PATH = os.path.join(BASE_DIR, "final_output.json")

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "tds hello!"}

@app.post("/api")
async def full_pipeline(question_file: UploadFile = File(...)):
    try:
        print("\n===== /api pipeline started =====")

        # Step 1: Save uploaded file
        file_bytes = await question_file.read()
        with open(QUESTION_PATH, "wb") as f:
            f.write(file_bytes)
        print(f"[Step 1] Saved question file → {QUESTION_PATH}")

        # Step 2: Run scraping
        print("[Step 2] Running DataScraping...")
        task_breakdown(file_path=QUESTION_PATH)

        # Step 3: Convert CSV → JSON
        print("[Step 3] Converting scraped CSV to JSON...")
        csv_to_json()
        if not os.path.exists(SCRAPED_PATH):
            raise HTTPException(status_code=500, detail="scraped_data.json not created")
        print(f"[Step 3] Conversion completed → {SCRAPED_PATH}")

        # Step 4: Generate code
        question_text = file_bytes.decode("utf-8")
        print("[Step 4] Generating code...")
        gen_result = answer.generate_and_save_code(question_text)
        if gen_result.get("status") != "success":
            return gen_result
        print("[Step 4] Code generation successful.")

        # Step 5: Run code & produce answer
        print("[Step 5] Running generated code...")
        run_result = answer.run_code_and_save_answer()
        if run_result.get("status") != "success":
            return run_result
        print("[Step 5] Execution finished.")

        # Step 6: Return final output
        if not os.path.exists(FINAL_OUTPUT_PATH):
            raise HTTPException(status_code=500, detail="final_output.json not found")
        with open(FINAL_OUTPUT_PATH, "r", encoding="utf-8") as f:
            final_answer = json.load(f)

        print("===== /api pipeline finished =====\n")

        return {
            "status": "success",
            "question_file": QUESTION_PATH,
            "scraped_json": SCRAPED_PATH,
            "answer": final_answer
        }

    except Exception as e:
        print(f"[Fatal Error] {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
