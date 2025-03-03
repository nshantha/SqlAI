import logging
import re
import os
from typing import Dict, List, Any, Optional, Tuple
import anthropic
import json
from app.config import DB_IDENTIFICATION_PATTERNS, DB_NAME_HEADER_PATTERN

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
        
        # Try to find database name in the schema information using the configured pattern
        import re
        match = re.search(DB_NAME_HEADER_PATTERN, db_schema, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Check for database-specific patterns from configuration
        db_schema_lower = db_schema.lower()
        for pattern, db_name in DB_IDENTIFICATION_PATTERNS.items():
            if re.search(pattern, db_schema_lower, re.IGNORECASE):
                return db_name
        
        # No specific database identified
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
                model="claude-3-5-sonnet-latest",
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
        
        # For small result sets (≤ 5), create a beautiful markdown table
        if len(results) <= 5:
            return self._create_beautiful_table(results, show_all_columns=True)
        
        # For medium result sets (6-20), show all rows but with limited details
        if len(results) <= 20:
            return self._create_beautiful_table(results, show_all_columns=True, abbreviate_values=True)
        
        # For larger result sets (> 20), show all rows but only ID columns
        return self._create_beautiful_table(results, show_all_columns=False)
    
    def _create_beautiful_table(self, results: List[Dict[str, Any]], 
                               show_all_columns: bool = True,
                               abbreviate_values: bool = False) -> str:
        """
        Create a beautiful markdown table from query results
        
        Args:
            results: Query results
            show_all_columns: Whether to show all columns or just ID columns
            abbreviate_values: Whether to abbreviate long values
            
        Returns:
            Formatted markdown table as string
        """
        if not results:
            return "The query returned no results."
        
        # Start with a summary
        summary = f"📊 **Query Results:** {len(results)} rows found\n\n"
        
        # Determine which columns to display
        if show_all_columns:
            columns = list(results[0].keys())
        else:
            # Show only ID, code, and name columns
            id_columns = [col for col in results[0].keys() if "id" in col.lower() or "code" in col.lower() or "name" in col.lower()]
            if not id_columns:
                # If no ID columns found, use the first column
                id_columns = [list(results[0].keys())[0]]
            columns = id_columns
        
        # Start markdown table
        summary += "| " + " | ".join([f"**{col}**" for col in columns]) + " |\n"
        summary += "| " + " | ".join(["---" for _ in columns]) + " |\n"
        
        # Add rows
        for row in results:
            row_values = []
            for col in columns:
                value = row.get(col, "")
                
                # Format the value based on its type and content
                str_value = str(value)
                
                # Highlight ID values
                if "id" in col.lower() and str_value.isdigit():
                    str_value = f"`{str_value}`"  # Code formatting for IDs
                
                # Format dates nicely
                elif "date" in col.lower() and str_value and str_value != "None":
                    # Try to make dates more readable if they look like dates
                    if "-" in str_value and len(str_value) >= 10:
                        str_value = f"📅 {str_value.split(' ')[0]}"
                
                # Abbreviate long values if requested
                elif abbreviate_values and len(str_value) > 20:
                    str_value = str_value[:17] + "..."
                
                # Add emoji indicators for certain columns
                if "status" in col.lower():
                    if "active" in str_value.lower():
                        str_value = "✅ " + str_value
                    elif "inactive" in str_value.lower() or "expired" in str_value.lower():
                        str_value = "❌ " + str_value
                
                row_values.append(str_value)
            
            summary += "| " + " | ".join(row_values) + " |\n"
        
        # Add note about expandability
        if len(results) > 5:
            summary += "\n> 💡 *The table above shows all " + ("rows" if len(results) <= 20 else "IDs") + ". Expand for more details.*\n"
        
        # For larger result sets, add detailed view of first 5 rows
        if len(results) > 5:
            # Use markdown formatting that's more likely to be preserved
            summary += "\n**Detailed View (First 5 Rows)**\n\n"
            summary += "```json\n" + json.dumps(results[:5], indent=2, default=str) + "\n```\n"
        
        return summary
    
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
- CRITICAL: ONLY return information that is directly retrieved from the database
- NEVER hallucinate or make up data that isn't explicitly returned from the query
- IMPORTANT: Always include the FULL table of results in your response exactly as provided
- Preserve the exact markdown formatting of the table including all emojis, formatting, and styling
- Preserve all code blocks with their triple backticks (```) exactly as provided
- Do not summarize or omit any rows from the table - show ALL results
- Copy and paste the entire table with its formatting intact
- If the results don't answer the user's question, state clearly "The database does not contain information to answer this question" - do not speculate or make up information
- If the results are empty, state clearly "The query returned no results" - do not speculate why
- Always clearly distinguish between factual data from the database and any explanations you provide
"""
        
        message_content = f"""
User question: {user_message}

I generated this SQL query:
```sql
{sql_query}
```

Query results:
{results_text}

IMPORTANT INSTRUCTIONS:
1. Copy and paste the COMPLETE table above in your response, preserving ALL formatting
2. Do not modify the table format, keep all markdown, emojis, and styling exactly as shown
3. Preserve all code blocks with their triple backticks (```) exactly as provided
4. Show ALL rows in the table - do not summarize or omit any data
5. ONLY provide information that is directly from the database results
6. If the results don't answer the question, state "The database does not contain this information" - do not speculate or make up information

"""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-latest",
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
        Generate a response when a SQL query fails
        
        Args:
            user_message: Original user question
            sql_query: SQL query that failed
            error_message: Error message from the database
            db_schema: Database schema information
            
        Returns:
            Response explaining the error
        """
        system_prompt = self._build_system_prompt(db_schema) + """
Additional instructions:
- I attempted to execute a SQL query but it failed
- CRITICAL: ONLY explain the specific error that occurred - do not speculate about data that might exist
- NEVER hallucinate or make up information about the database contents
- Explain the error in technical terms based solely on the error message
- If you cannot determine the cause from the error message, state "I cannot determine the exact cause of this error without more information"
- Do not make assumptions about what data might be in the database
- Focus only on explaining why the query syntax or structure caused the error
"""
        
        message_content = f"""
User question: {user_message}

I generated this SQL query:
```sql
{sql_query}
```

The query failed with this error:
```
{error_message}
```

Please explain what went wrong with the query and how to fix it. Do not speculate about what data might exist in the database - focus only on the technical error.
"""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-latest",
                system=system_prompt,
                messages=[{"role": "user", "content": message_content}],
                max_tokens=2000,
                temperature=0.1
            )
            
            return response.content[0].text
            
        except Exception as e:
            logger.error(f"Error generating error response: {e}")
            return f"I tried to run this query:\n```sql\n{sql_query}\n```\n\nBut it failed with this error:\n```\n{error_message}\n```\n\nI apologize, but I encountered an error while trying to explain the issue: {str(e)}"