import asyncio
import json
import logging
import subprocess
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class MCPClient:
    """Client for interacting with MCP PostgreSQL server"""
    
    def __init__(self, server_path: str, connection_string: str):
        """
        Initialize MCP client
        
        Args:
            server_path: Path to the MCP PostgreSQL server executable
            connection_string: PostgreSQL connection string
        """
        self.server_path = server_path
        self.connection_string = connection_string
        self.process = None
        self.request_id = 0
    
    async def start_server(self):
        """Start the MCP PostgreSQL server"""
        if self.process is not None:
            logger.warning("MCP server is already running")
            return
        
        try:
        # For NPX usage
        if self.server_path == "npx":
            # Start the server process with npx
            self.process = await asyncio.create_subprocess_exec(
                "npx", "-y", "@modelcontextprotocol/server-postgres", self.connection_string,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Read stderr in a separate task
            async def read_stderr():
                while True:
                    line = await self.process.stderr.readline()
                    if not line:
                        break
                    logger.error(f"MCP Server stderr: {line.decode().strip()}")
            
            asyncio.create_task(read_stderr())
            
            # For Python package
            elif self.server_path.endswith("mcp-server-postgres"):
                # Try to find the executable using which
                import shutil
                server_path = shutil.which("mcp-server-postgres")
                if server_path:
                    self.process = await asyncio.create_subprocess_exec(
                        server_path, self.connection_string,
                        stdin=asyncio.subprocess.PIPE,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                else:
                    # Try using python -m approach as fallback
                    self.process = await asyncio.create_subprocess_exec(
                        "python", "-m", "mcp_server_postgres", self.connection_string,
                        stdin=asyncio.subprocess.PIPE,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
            else:
                # Use the specified path directly
                self.process = await asyncio.create_subprocess_exec(
                    self.server_path, self.connection_string,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
            
            logger.info(f"Started MCP PostgreSQL server (PID: {self.process.pid})")
            
            # Initialize MCP connection
            await self._initialize()
            
        except Exception as e:
            logger.error(f"Failed to start MCP server: {e}")
            await self.stop_server()
            raise
    
    async def _initialize(self):
        """Initialize the MCP connection"""
        # Send initialize request
        init_request = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "initialize",
            "params": {
                "name": "pg-chat-client",
                "version": "1.0.0",
                "protocolVersion": "0.1.0",
                "capabilities": {}
            }
        }
        
        response = await self._send_request(init_request)
        logger.info("MCP server initialized")
        
        # Send initialized notification
        initialized_notification = {
            "jsonrpc": "2.0",
            "method": "initialized",
            "params": {}
        }
        
        await self._send_notification(initialized_notification)
    
    async def stop_server(self):
        """Stop the MCP PostgreSQL server"""
        if self.process is None:
            return
        
        try:
            # Try to send shutdown request
            shutdown_request = {
                "jsonrpc": "2.0",
                "id": self._next_id(),
                "method": "shutdown",
                "params": {}
            }
            
            await self._send_request(shutdown_request)
            
            # Send exit notification
            exit_notification = {
                "jsonrpc": "2.0",
                "method": "exit",
                "params": {}
            }
            
            await self._send_notification(exit_notification)
            
        except Exception as e:
            logger.warning(f"Error during graceful shutdown: {e}")
        
        # Terminate the process if it's still running
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
        
        self.process = None
        logger.info("MCP server stopped")
    
    async def list_resources(self) -> List[Dict[str, Any]]:
        """List available database resources"""
        request = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "resources/list",
            "params": {}
        }
        
        response = await self._send_request(request)
        return response.get("result", {}).get("resources", [])
    
    async def list_tools(self) -> List[Dict[str, Any]]:
        """List available database tools"""
        request = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "tools/list",
            "params": {}
        }
        
        response = await self._send_request(request)
        return response.get("result", {}).get("tools", [])
    
    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call an MCP tool
        
        Args:
            name: Tool name
            arguments: Tool arguments
            
        Returns:
            Tool execution result
        """
        request = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments
            }
        }
        
        response = await self._send_request(request)
        return response.get("result", {})
    
    async def read_resource(self, uri: str) -> Dict[str, Any]:
        """
        Read a resource
        
        Args:
            uri: Resource URI
            
        Returns:
            Resource content
        """
        request = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "resources/read",
            "params": {
                "uri": uri
            }
        }
        
        response = await self._send_request(request)
        return response.get("result", {})
    
    async def _send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request to the MCP server and wait for response"""
        if not self.process or self.process.stdin.is_closing():
            raise RuntimeError("MCP server process is not running")
        
        request_json = json.dumps(request) + "\n"
        self.process.stdin.write(request_json.encode())
        await self.process.stdin.drain()
        
        response_line = await self.process.stdout.readline()
        if not response_line:
            raise RuntimeError("MCP server process closed unexpectedly")
        
        response = json.loads(response_line)
        
        if "error" in response:
            error = response["error"]
            logger.error(f"MCP error: {error}")
            raise RuntimeError(f"MCP error: {error['message']}")
        
        return response
    
    async def _send_notification(self, notification: Dict[str, Any]):
        """Send a notification to the MCP server (no response expected)"""
        if not self.process or self.process.stdin.is_closing():
            raise RuntimeError("MCP server process is not running")
        
        notification_json = json.dumps(notification) + "\n"
        self.process.stdin.write(notification_json.encode())
        await self.process.stdin.drain()
    
    def _next_id(self) -> int:
        """Generate next request ID"""
        self.request_id += 1
        return self.request_id