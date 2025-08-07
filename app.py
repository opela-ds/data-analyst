from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import json
import os

from dotenv import load_dotenv
import google.generativeai as genai

from DataScraping.datascraper import task_breakdown
from answer import generate_and_save_code, run_code_and_save_answer

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")

if not api_key:
    raise ValueError("GEMINI_API_KEY not found in .env file")

genai.configure(api_key=api_key)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "hello"}

@app.post("/data_scrape")
async def process_file():
    try:
        result = task_breakdown()
        with open("DataScraping/generated_scraper.py", 'w', encoding="utf-8") as fp:
            fp.write(result)
        return {"status": "success", "message": "Scraped response saved"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/answer")
async def answer_questions():
    gen_result = generate_and_save_code()
    if gen_result.get("status") != "success":
        return gen_result

    run_result = run_code_and_save_answer()

    if run_result.get("status") == "success":
        try:
            with open("output_answer.json", "r", encoding="utf-8") as f:
                final_answer = json.load(f)
            return {"status": "success", "answer": final_answer}
        except Exception as e:
            return {"status": "error", "message": f"Failed to read final answer from file: {e}"}
    else:
        return run_result

if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
