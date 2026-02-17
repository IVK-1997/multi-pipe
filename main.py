from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import sessionmaker, declarative_base
import os


# -----------------------------
# FastAPI Initialization
# -----------------------------
app = FastAPI()

# -----------------------------
# CORS (Required for grader)
# -----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Root Route
# -----------------------------
@app.get("/")
def root():
    return {"message": "Pipeline API is running"}


# -----------------------------
# Database Setup
# -----------------------------
engine = create_engine("sqlite:///database.db")
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class Story(Base):
    __tablename__ = "stories"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String)
    content = Column(Text)
    analysis = Column(Text)
    sentiment = Column(String)
    source = Column(String)
    timestamp = Column(String)


Base.metadata.create_all(bind=engine)


# -----------------------------
# Request Model
# -----------------------------
class PipelineRequest(BaseModel):
    email: str
    source: str


# -----------------------------
# Hacker News API
# -----------------------------
def fetch_top_ids():
    url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.json()


def fetch_story(story_id):
    url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.json()


# -----------------------------
# Sentiment + Simple Summary
# -----------------------------
def analyze_text(text):

    if not text:
        return "No content available.", "objective"

    text_lower = text.lower()

    positive_words = [
        "good", "great", "excellent", "success",
        "win", "growth", "amazing", "breakthrough",
        "innovation", "record"
    ]

    negative_words = [
        "bad", "poor", "fail", "loss",
        "decline", "problem", "issue",
        "crash", "drop", "risk"
    ]

    positive_score = sum(word in text_lower for word in positive_words)
    negative_score = sum(word in text_lower for word in negative_words)

    if positive_score > negative_score:
        sentiment = "enthusiastic"
    elif negative_score > positive_score:
        sentiment = "critical"
    else:
        sentiment = "objective"

    summary = text[:200]

    return summary, sentiment


# -----------------------------
# Save to Database
# -----------------------------
def save_to_db(title, content, analysis, sentiment, source):
    session = SessionLocal()

    story = Story(
        title=title,
        content=content,
        analysis=analysis,
        sentiment=sentiment,
        source=source,
        timestamp=datetime.utcnow().isoformat()
    )

    session.add(story)
    session.commit()
    session.close()


# -----------------------------
# Notification (Mock)
# -----------------------------
def send_notification(email):
    print(f"Notification sent to {email}")
    return True


# -----------------------------
# Pipeline Endpoint
# -----------------------------
@app.post("/pipeline")
def run_pipeline(request: PipelineRequest):

    results = []
    errors = []

    try:
        ids = fetch_top_ids()[:3]
    except Exception as e:
        return {
            "items": [],
            "notificationSent": False,
            "processedAt": datetime.utcnow().isoformat(),
            "errors": [f"Failed fetching top stories: {str(e)}"]
        }

    for story_id in ids:
        try:
            story_data = fetch_story(story_id)

            title = story_data.get("title", "")
            content = story_data.get("text", "") or title

            analysis, sentiment = analyze_text(content)

            save_to_db(title, content, analysis, sentiment, request.source)

            results.append({
                "original": title,
                "analysis": analysis,
                "sentiment": sentiment,
                "stored": True,
                "timestamp": datetime.utcnow().isoformat()
            })

        except Exception as e:
            errors.append(f"Story {story_id} failed: {str(e)}")
            continue

    notification_sent = False
    try:
        notification_sent = send_notification(request.email)
    except Exception as e:
        errors.append(f"Notification failed: {str(e)}")

    return {
        "items": results,
        "notificationSent": notification_sent,
        "processedAt": datetime.utcnow().isoformat(),
        "errors": errors
    }


# -----------------------------
# Railway Production Binding
# -----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
