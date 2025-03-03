import json
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def format_schema_for_llm(resources: List[Dict[str, Any]]) -> str:
    """
    Format database schema information for the LLM
    
    Args:
        resources: List of database resources
        
    Returns:
        Formatted schema information
    """
    if not resources:
        return "No database schema information available."
    
    schema_info = []
    
    for resource in resources:
        uri = resource.get("uri", "")
        name = resource.get("name", "Unknown")
        description = resource.get("description", "")
        
        # Skip resources that don't look like database objects
        if not (uri.startswith("postgres://") or "table" in uri or "schema" in uri):
            continue
            
        schema_info.append(f"- {name}: {description}")
        
        # Add URI for reference
        schema_info.append(f"  URI: {uri}")
        schema_info.append("")
    
    if not schema_info:
        return "No relevant database schema information available."
    
    return "\n".join(schema_info)

async def extract_db_schema(mcp_client, format_for_llm=True) -> str:
    """
    Extract database schema information using MCP client
    
    Args:
        mcp_client: MCP client instance
        format_for_llm: Whether to format the schema for LLM consumption
        
    Returns:
        Database schema information as string
    """
    try:
        # List available resources
        resources = await mcp_client.list_resources()
        
        if format_for_llm:
            return format_schema_for_llm(resources)
        
        return json.dumps(resources, indent=2)
        
    except Exception as e:
        logger.error(f"Error extracting database schema: {e}")
        return "Error extracting database schema."

def format_conversation_for_frontend(conversation: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Format conversation history for frontend display
    
    Args:
        conversation: Conversation history
        
    Returns:
        Formatted conversation for frontend
    """
    formatted = []
    
    for message in conversation:
        role = message.get("role", "")
        content = message.get("content", "")
        
        # Convert role to display name
        display_name = "You" if role == "user" else "Assistant"
        
        formatted.append({
            "role": role,
            "display_name": display_name,
            "content": content
        })
    
    return formatted