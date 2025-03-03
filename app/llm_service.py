import logging
import re
import os
from typing import Dict, List, Any, Optional, Tuple
import anthropic
import json

logger = logging.getLogger(__name__)

class LLMService:
    """Service for interacting with Claude API"""
    
    def __init__(self, api_key: str):
        """
        Initialize LLM service
        
        Args:
            api_key: Anthropic API key
        """
        self.client = anthropic.Anthropic(api_key=api_key)
        
        # Load prompts from files
        self.base_prompt = self._load_prompt_file("app/prompts/base_prompt.txt")
        self.default_db_prompt = self._load_prompt_file("app/prompts/databases/default.txt")
        self.db_specific_prompts = self._load_db_prompts()
    
    def _load_prompt_file(self, file_path: str) -> str:
        """
        Load prompt from a file
        
        Args:
            file_path: Path to the prompt file
            
        Returns:
            Prompt text
        """
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    return f.read().strip()
            else:
                logger.warning(f"Prompt file not found: {file_path}")
                return ""
        except Exception as e:
            logger.error(f"Error loading prompt file {file_path}: {e}")
            return ""
    
    def _load_db_prompts(self) -> Dict[str, str]:
        """
        Load all database-specific prompts
        
        Returns:
            Dictionary of database name to prompt text
        """
        prompts = {}
        db_prompts_dir = "app/prompts/databases"
        
        try:
            if os.path.exists(db_prompts_dir):
                for filename in os.listdir(db_prompts_dir):
                    if filename.endswith(".txt") and filename != "default.txt":
                        db_name = filename.replace(".txt", "")
                        file_path = os.path.join(db_prompts_dir, filename)
                        prompts[db_name] = self._load_prompt_file(file_path)
            return prompts
        except Exception as e:
            logger.error(f"Error loading database prompts: {e}")
            return {}

    def _build_system_prompt(self, db_schema: Optional[str] = None) -> str:
        """
        Build the system prompt with database context
        
        Args:
            db_schema: Database schema information
            
        Returns:
            Formatted system prompt
        """
        # Start with the base prompt
        system_prompt = self.base_prompt
        
        # Add database-specific instructions if available
        db_name = self._extract_db_name_from_schema(db_schema)
        if db_name and db_name in self.db_specific_prompts:
            system_prompt += "\n\n" + self.db_specific_prompts[db_name]
        else:
            # Use default database prompt if specific one not found
            system_prompt += "\n\n" + self.default_db_prompt
        
        # Add database schema information if available
        if db_schema:
            system_prompt += "\n\nDatabase Schema Information:\n" + db_schema
        
        return system_prompt
    
    def _extract_db_name_from_schema(self, db_schema: Optional[str]) -> Optional[str]:
        """
        Extract database name from schema information
        
        Args:
            db_schema: Database schema information
            
        Returns:
            Database name if found, None otherwise
        """
        if not db_schema:
            return None
        
        # Try to find database name in the schema information
        import re
        match = re.search(r"Database Schema (?:for|of)?\s+([a-zA-Z0-9_]+)", db_schema, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Check if promo_tracker is mentioned
        if "promo_tracker" in db_schema.lower():
            return "promo_tracker_db"
        
        return None
    
    async def generate_response(
        self, 
        user_message: str,
        db_schema: Optional[str] = None,
        db_client = None,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Tuple[str, Optional[str]]:
        """
        Generate a response using Claude
        
        Args:
            user_message: The user's question about the database
            db_schema: Database schema information if available
            db_client: Database client for executing queries
            conversation_history: Previous conversation messages
            
        Returns:
            Tuple of (Claude's response, SQL query if generated)
        """
        if conversation_history is None:
            conversation_history = []
        
        # Construct the system prompt
        system_prompt = self._build_system_prompt(db_schema)
        
        # Build messages
        messages = [{"role": "user", "content": user_message}]
        
        # Add conversation history if available
        if conversation_history:
            # Limit history to last 10 messages to avoid token limits
            recent_history = conversation_history[-10:]
            messages = recent_history + messages
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-haiku-latest",
                system=system_prompt,
                messages=messages,
                max_tokens=4000,
                temperature=0.1
            )
            
            response_text = response.content[0].text
            
            # Extract SQL query if present
            sql_query = self._extract_sql_query(response_text)
            
            # If a SQL query was extracted and we have a database client, execute it
            if sql_query and db_client:
                try:
                    # Execute the query
                    results = await db_client.execute_query(sql_query)
                    
                    # Format the results
                    results_text = self._format_query_results(results)
                    
                    # Generate a new response with the query results
                    augmented_response = await self._generate_response_with_results(
                        user_message, sql_query, results_text, db_schema
                    )
                    
                    return augmented_response, sql_query
                    
                except Exception as e:
                    # If query execution fails, let Claude explain the error
                    error_response = await self._generate_error_response(
                        user_message, sql_query, str(e), db_schema
                    )
                    return error_response, sql_query
            
            return response_text, sql_query
            
        except Exception as e:
            logger.error(f"Error generating response from Claude: {e}")
            return f"I apologize, but I encountered an error: {str(e)}", None
    
    def _extract_sql_query(self, text: str) -> Optional[str]:
        """
        Extract SQL query from response text
        
        Args:
            text: Response text
            
        Returns:
            Extracted SQL query or None
        """
        # Look for SQL code blocks
        sql_pattern = r"```sql\s+(.*?)\s+```"
        matches = re.findall(sql_pattern, text, re.DOTALL)
        
        if matches:
            # Return the first SQL query found
            return matches[0].strip()
        
        return None
    
    def _format_query_results(self, results: List[Dict[str, Any]]) -> str:
        """
        Format query results for inclusion in prompt
        
        Args:
            results: Query results
            
        Returns:
            Formatted results as string
        """
        if not results:
            return "The query returned no results."
        
        # For small result sets, use JSON formatting
        if len(results) <= 20:
            return json.dumps(results, indent=2, default=str)
        
        # For larger result sets, provide a summary
        return f"The query returned {len(results)} rows. First 5 rows:\n{json.dumps(results[:5], indent=2, default=str)}"
    
    async def _generate_response_with_results(
        self, 
        user_message: str, 
        sql_query: str, 
        results_text: str,
        db_schema: Optional[str] = None
    ) -> str:
        """
        Generate a response that includes query results
        
        Args:
            user_message: Original user question
            sql_query: SQL query that was executed
            results_text: Formatted query results
            db_schema: Database schema information
            
        Returns:
            Response with query results
        """
        system_prompt = self._build_system_prompt(db_schema) + """
Additional instructions:
- I've executed the SQL query you generated and will provide the results
- Analyze the query results and provide insights
- Explain the data in a way that directly answers the user's question
- Format tables or lists nicely when presenting numeric data
- If the results don't fully address the question, suggest improvements
"""
        
        message_content = f"""
User question: {user_message}

I generated this SQL query:
```sql
{sql_query}
```

Query results:
```json
{results_text}
```

Please help me analyze these results and answer the user's question.
"""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-haiku-latest",
                system=system_prompt,
                messages=[{"role": "user", "content": message_content}],
                max_tokens=4000,
                temperature=0.1
            )
            
            return response.content[0].text
            
        except Exception as e:
            logger.error(f"Error generating response with results: {e}")
            return f"I ran this query:\n```sql\n{sql_query}\n```\n\nResults:\n```\n{results_text}\n```\n\nI apologize, but I encountered an error analyzing the results: {str(e)}"
    
    async def _generate_error_response(
        self, 
        user_message: str, 
        sql_query: str, 
        error_message: str,
        db_schema: Optional[str] = None
    ) -> str:
        """
        Generate a response explaining a SQL error
        
        Args:
            user_message: Original user question
            sql_query: SQL query that failed
            error_message: Error message from database
            db_schema: Database schema information
            
        Returns:
            Response explaining the error
        """
        system_prompt = self._build_system_prompt(db_schema) + """
Additional instructions:
- I attempted to execute the SQL query you generated, but it resulted in an error
- Explain what went wrong and how to fix it
- Suggest an improved query if possible
- Be helpful and educational about the error
"""
        
        message_content = f"""
User question: {user_message}

I generated this SQL query:
```sql
{sql_query}
```

But it resulted in this error:
{error_message}

Please explain what went wrong and how to fix it.
"""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-haiku-latest",
                system=system_prompt,
                messages=[{"role": "user", "content": message_content}],
                max_tokens=4000,
                temperature=0.1
            )
            
            return response.content[0].text
            
        except Exception as e:
            logger.error(f"Error generating error response: {e}")
            return f"I tried to run this query:\n```sql\n{sql_query}\n```\n\nBut it failed with error: {error_message}\n\nI apologize, but I encountered an additional error while analyzing this issue: {str(e)}"