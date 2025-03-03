import logging
import os
import traceback
from typing import Dict, List, Any, Optional
import asyncio

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.config import (
    ANTHROPIC_API_KEY, DB_JDBC_URL, DB_USER, DB_PASSWORD
)
from app.db_client import DatabaseClient
from app.llm_service import LLMService
from app.utils import format_conversation_for_frontend

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create FastAPI application
app = FastAPI(title="SQL Chat Assistant")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Set up Jinja2 templates
templates = Jinja2Templates(directory="templates")

# Initialize services
db_client = DatabaseClient(DB_JDBC_URL, DB_USER, DB_PASSWORD)
llm_service = LLMService(ANTHROPIC_API_KEY)

# Store conversation history (in memory for simplicity)
# For production, use a database or Redis
conversation_store = {}


# Models
class ChatMessage(BaseModel):
    content: str


@app.on_event("startup")
async def startup_event():
    """Connect to the database on application startup"""
    try:
        # Test database connection
        success, message = await db_client.test_connection()
        if success:
            logger.info(f"Database connection successful: {message}")
            
            # Try to get schema information
            try:
                schema = await db_client.format_schema_for_llm()
                logger.info(f"Retrieved database schema successfully")
            except Exception as e:
                logger.error(f"Error retrieving database schema: {str(e)}")
        else:
            logger.error(f"Database connection failed: {message}")
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")
        logger.error(traceback.format_exc())


@app.on_event("shutdown")
async def shutdown_event():
    """Disconnect from the database on application shutdown"""
    await db_client.disconnect()
    logger.info("Database disconnected")


@app.get("/", response_class=HTMLResponse)
async def get_homepage():
    """Serve the chat interface"""
    with open("templates/index.html", "r") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)


@app.get("/api/database/info")
async def get_database_info():
    """Get database schema information"""
    try:
        schema_info = await db_client.format_schema_for_llm()
        return {"schema": schema_info}
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/chat/{session_id}")
async def chat_message(session_id: str, message: ChatMessage):
    """Handle a chat message"""
    try:
        # Get or create conversation history
        if session_id not in conversation_store:
            conversation_store[session_id] = []
        
        # Add user message to history
        conversation_store[session_id].append({
            "role": "user",
            "content": message.content
        })
        
        # Get database schema
        db_schema = await db_client.format_schema_for_llm()
        
        # Generate response
        response, sql_query = await llm_service.generate_response(
            user_message=message.content,
            db_schema=db_schema,
            db_client=db_client,
            conversation_history=conversation_store[session_id]
        )
        
        # Add assistant response to history
        conversation_store[session_id].append({
            "role": "assistant",
            "content": response
        })
        
        # Format conversation for frontend
        formatted_conversation = format_conversation_for_frontend(
            conversation_store[session_id]
        )
        
        return {
            "response": response,
            "sql_query": sql_query,
            "conversation": formatted_conversation
        }
        
    except Exception as e:
        logger.error(f"Error in chat: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time chat"""
    await websocket.accept()
    
    # Get or create conversation history
    if session_id not in conversation_store:
        conversation_store[session_id] = []
    
    try:
        # Send initial conversation history
        formatted_conversation = format_conversation_for_frontend(
            conversation_store[session_id]
        )
        await websocket.send_json({"type": "history", "conversation": formatted_conversation})
        
        # Process messages
        while True:
            data = await websocket.receive_text()
            message = {"role": "user", "content": data}
            
            # Add to conversation history
            conversation_store[session_id].append(message)
            
            # Get database schema
            db_schema = await db_client.format_schema_for_llm()
            
            # Send typing indicator
            await websocket.send_json({"type": "typing", "status": True})
            
            # Generate response
            response, sql_query = await llm_service.generate_response(
                user_message=data,
                db_schema=db_schema,
                db_client=db_client,
                conversation_history=conversation_store[session_id]
            )
            
            # Add to conversation history
            assistant_message = {"role": "assistant", "content": response}
            conversation_store[session_id].append(assistant_message)
            
            # Stop typing indicator
            await websocket.send_json({"type": "typing", "status": False})
            
            # Send response
            await websocket.send_json({
                "type": "message",
                "message": {
                    "role": "assistant",
                    "display_name": "Assistant",
                    "content": response
                },
                "sql_query": sql_query
            })
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for session {session_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        logger.error(traceback.format_exc())
        await websocket.send_json({"type": "error", "message": str(e)})


@app.post("/api/query/execute")
async def execute_query(query: dict):
    """Execute a SQL query directly"""
    try:
        if not query.get("sql"):
            raise HTTPException(status_code=400, detail="No SQL query provided")
        
        sql = query["sql"]
        results = await db_client.execute_query(sql)
        
        return {
            "success": True,
            "results": results
        }
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    from app.config import APP_HOST, APP_PORT, DEBUG
    
    uvicorn.run("app.main:app", host=APP_HOST, port=APP_PORT, reload=DEBUG)