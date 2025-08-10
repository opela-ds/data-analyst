import os
import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from DataScraping.datascraper import task_breakdown
import answer
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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

@app.post("/data_scrape")
async def data_scrape_endpoint(
    questions_path: str,  # Now expects path string, not UploadFile
    attachments: Optional[List[UploadFile]] = File(default=None)
):
    try:
        # Save attachments if provided
        if attachments:
            upload_dir = os.path.join(BASE_DIR, "uploads")
            os.makedirs(upload_dir, exist_ok=True)
            for file in attachments:
                file_path = os.path.join(upload_dir, file.filename)
                with open(file_path, "wb") as f_out:
                    f_out.write(await file.read())

        task_breakdown(file_path=questions_path)

        return {
            "status": "success",
            "message": "Files uploaded and scraper executed"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/answer")
async def answer_questions(
    question_file: UploadFile = File(None),
    question_text: str = Form(None)
):
    if not question_file and not question_text:
        raise HTTPException(status_code=400, detail="Provide either question_file or question_text")

    if question_file:
        question_text = (await question_file.read()).decode("utf-8")

    gen_result = answer.generate_and_save_code(question_text)
    if gen_result.get("status") != "success":
        return gen_result

    run_result = answer.run_code_and_save_answer()
    if run_result.get("status") != "success":
        return run_result

    final_answer_path = os.path.join(BASE_DIR, "final_output.json")

    try:
        with open(final_answer_path, "r", encoding="utf-8") as f:
            final_answer = json.load(f)
        return {"status": "success", "answer": final_answer}
    except FileNotFoundError:
        return {"status": "error", "answer": {}, "message": "final_output.json not found"}
    except json.JSONDecodeError as e:
        return {"status": "error", "answer": {}, "message": f"Invalid JSON in final_output.json: {e}"}
    except Exception as e:
        return {"status": "error", "answer": {}, "message": f"Failed to read final answer: {e}"}

@app.post("/api/")
async def full_pipeline(
    questions: UploadFile = File(...),
    attachments: Optional[List[UploadFile]] = File(default=None)
):
    question_path = os.path.join(BASE_DIR, "question.txt")
    content = await questions.read()
    with open(question_path, "wb") as f:
        f.write(content)

    scrape_response = await data_scrape_endpoint(
        questions_path=question_path,
        attachments=attachments
    )
    if scrape_response.get("status") != "success":
        return scrape_response

    question_text = content.decode("utf-8")

    answer_response = await answer_questions(question_text=question_text)
    return answer_response

if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
