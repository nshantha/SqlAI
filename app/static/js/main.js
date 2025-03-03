// Main JavaScript for MCP Database Chat

document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const conversationEl = document.getElementById('conversation');
    const messageInput = document.getElementById('message-input');
    const sendButton = document.getElementById('send-button');
    const connectionStatus = document.getElementById('connection-status');
    const dbInfoBtn = document.getElementById('db-info-btn');
    const dbInfoModal = document.getElementById('db-info-modal');
    const closeModalBtn = document.getElementById('close-modal');
    const dbInfoContent = document.getElementById('db-info-content');
    
    // Session ID - Generate a random one for this session
    const sessionId = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    
    // WebSocket connection
    let socket;
    let isConnected = false;
    
    // Typing indicator state
    let isTyping = false;
    
    // Connect to WebSocket
    function connectWebSocket() {
        updateConnectionStatus('connecting');
        
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/${sessionId}`;
        
        socket = new WebSocket(wsUrl);
        
        socket.onopen = () => {
            isConnected = true;
            updateConnectionStatus('connected');
            console.log('WebSocket connected');
        };
        
        socket.onmessage = (event) => {
            const data = JSON.parse(event.data);
            handleWebSocketMessage(data);
        };
        
        socket.onclose = () => {
            isConnected = false;
            updateConnectionStatus('disconnected');
            console.log('WebSocket disconnected');
            
            // Try to reconnect after a delay
            setTimeout(() => {
                if (!isConnected) {
                    connectWebSocket();
                }
            }, 3000);
        };
        
        socket.onerror = (error) => {
            console.error('WebSocket error:', error);
            updateConnectionStatus('disconnected');
        };
    }
    
    // Handle messages from WebSocket
    function handleWebSocketMessage(data) {
        switch (data.type) {
            case 'message':
                // Add message to conversation
                addMessage(data.message);
                break;
                
            case 'typing':
                // Update typing indicator
                updateTypingIndicator(data.status);
                break;
                
            case 'history':
                // Load conversation history
                loadConversationHistory(data.conversation);
                break;
                
            case 'error':
                // Handle error
                showError(data.message);
                break;
                
            default:
                console.warn('Unknown message type:', data.type);
        }
    }
    
    // Update connection status
    function updateConnectionStatus(status) {
        connectionStatus.className = 'px-2 py-1 rounded text-xs';
        
        switch (status) {
            case 'connected':
                connectionStatus.textContent = 'Connected';
                connectionStatus.classList.add('bg-green-500');
                break;
                
            case 'disconnected':
                connectionStatus.textContent = 'Disconnected';
                connectionStatus.classList.add('bg-red-500');
                break;
                
            case 'connecting':
                connectionStatus.textContent = 'Connecting...';
                connectionStatus.classList.add('bg-yellow-500');
                break;
        }
    }
    
    // Add a message to the conversation
    function addMessage(message) {
        const messageEl = document.createElement('div');
        messageEl.className = `message ${message.role}`;
        
        const senderEl = document.createElement('div');
        senderEl.className = 'sender';
        senderEl.textContent = message.display_name;
        
        const contentEl = document.createElement('div');
        contentEl.className = 'content';
        
        // Process content for markdown-like formatting
        const formattedContent = formatMessageContent(message.content);
        contentEl.innerHTML = formattedContent;
        
        messageEl.appendChild(senderEl);
        messageEl.appendChild(contentEl);
        
        conversationEl.appendChild(messageEl);
        
        // Scroll to bottom
        scrollToBottom();
    }
    
    // Format message content with basic markdown support
    function formatMessageContent(content) {
        if (!content) return '';
        
        // Replace code blocks
        content = content.replace(/```([\s\S]*?)```/g, function(match, code) {
            return `<pre><code>${escapeHtml(code.trim())}</code></pre>`;
        });
        
        // Replace inline code
        content = content.replace(/`([^`]+)`/g, '<code>$1</code>');
        
        // Replace line breaks
        content = content.replace(/\n/g, '<br>');
        
        return content;
    }
    
    // Escape HTML special characters
    function escapeHtml(text) {
        return text
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }
    
    // Update the typing indicator
    function updateTypingIndicator(isActive) {
        // Remove existing indicator
        const existingIndicator = document.querySelector('.typing-indicator');
        if (existingIndicator) {
            existingIndicator.remove();
        }
        
        if (isActive) {
            const indicatorEl = document.createElement('div');
            indicatorEl.className = 'typing-indicator';
            
            const textEl = document.createElement('span');
            textEl.textContent = 'Assistant is typing ';
            
            const dotsEl = document.createElement('div');
            dotsEl.className = 'dots';
            
            for (let i = 0; i < 3; i++) {
                const dotEl = document.createElement('div');
                dotEl.className = 'dot';
                dotsEl.appendChild(dotEl);
            }
            
            indicatorEl.appendChild(textEl);
            indicatorEl.appendChild(dotsEl);
            
            conversationEl.appendChild(indicatorEl);
            scrollToBottom();
        }
    }
    
    // Load conversation history
    function loadConversationHistory(conversation) {
        // Clear existing messages
        while (conversationEl.firstChild) {
            if (conversationEl.firstChild.classList && conversationEl.firstChild.classList.contains('welcome-message')) {
                break;
            }
            conversationEl.removeChild(conversationEl.firstChild);
        }
        
        // Add messages from history
        conversation.forEach(message => {
            addMessage(message);
        });
    }
    
    // Show error message
    function showError(message) {
        const errorEl = document.createElement('div');
        errorEl.className = 'bg-red-100 border-l-4 border-red-500 text-red-700 p-4 mb-4';
        errorEl.textContent = `Error: ${message}`;
        
        conversationEl.appendChild(errorEl);
        scrollToBottom();
    }
    
    // Scroll conversation to bottom
    function scrollToBottom() {
        conversationEl.scrollTop = conversationEl.scrollHeight;
    }
    
    // Send a message
    function sendMessage() {
        const message = messageInput.value.trim();
        
        if (!message) return;
        
        if (isConnected) {
            // Add user message to conversation
            addMessage({
                role: 'user',
                display_name: 'You',
                content: message
            });
            
            // Send message via WebSocket
            socket.send(message);
            
            // Clear input
            messageInput.value = '';
            
        } else {
            // If not connected, show error and try to reconnect
            showError('Not connected to server. Trying to reconnect...');
            connectWebSocket();
        }
    }
    
    // Load database information
    async function loadDatabaseInfo() {
        try {
            // Show loading state
            dbInfoContent.innerHTML = '<p>Loading database information...</p>';
            
            // Fetch database info
            const response = await fetch('/api/database/info');
            const data = await response.json();
            
            if (!data.schema) {
                dbInfoContent.innerHTML = '<p>No database information available.</p>';
                return;
            }
            
            // Parse and display schema
            try {
                const schema = JSON.parse(data.schema);
                displayDatabaseSchema(schema);
            } catch (e) {
                // If it's not valid JSON, display as text
                dbInfoContent.innerHTML = `<pre>${data.schema}</pre>`;
            }
            
        } catch (error) {
            console.error('Error loading database info:', error);
            dbInfoContent.innerHTML = `<p class="text-red-600">Error loading database information: ${error.message}</p>`;
        }
    }
    
    // Display database schema
    function displayDatabaseSchema(schema) {
        if (!Array.isArray(schema) || schema.length === 0) {
            dbInfoContent.innerHTML = '<p>No database schema information available.</p>';
            return;
        }
        
        let html = '<h3 class="text-lg font-bold mb-4">Database Schema</h3>';
        
        // Group by schema/table
        const tableGroups = {};
        
        schema.forEach(resource => {
            const uri = resource.uri || '';
            const name = resource.name || 'Unknown';
            const description = resource.description || '';
            
            // Extract schema and table information
            let schemaName = 'default';
            let tableName = name;
            
            if (uri.includes('table')) {
                const parts = uri.split('/');
                const tableIndex = parts.findIndex(p => p === 'table');
                
                if (tableIndex > 0 && parts.length > tableIndex + 1) {
                    schemaName = parts[tableIndex - 1];
                    tableName = parts[tableIndex + 1];
                }
            }
            
            if (!tableGroups[schemaName]) {
                tableGroups[schemaName] = [];
            }
            
            tableGroups[schemaName].push({
                name: tableName,
                description,
                uri
            });
        });
        
        // Build HTML for each schema
        Object.keys(tableGroups).sort().forEach(schemaName => {
            html += `<div class="mb-6">
                <h4 class="text-md font-bold mb-2">Schema: ${schemaName}</h4>
                <div class="pl-4 border-l-2 border-blue-200">`;
            
            // Add tables
            tableGroups[schemaName].sort((a, b) => a.name.localeCompare(b.name)).forEach(table => {
                html += `<div class="mb-3">
                    <h5 class="font-semibold">${table.name}</h5>
                    ${table.description ? `<p class="text-sm text-gray-600 mb-1">${table.description}</p>` : ''}
                    <p class="text-xs text-gray-500">${table.uri}</p>
                </div>`;
            });
            
            html += `</div></div>`;
        });
        
        dbInfoContent.innerHTML = html;
    }
    
    // Event listeners
    sendButton.addEventListener('click', sendMessage);
    
    messageInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            sendMessage();
        }
    });
    
    dbInfoBtn.addEventListener('click', () => {
        dbInfoModal.classList.remove('hidden');
        loadDatabaseInfo();
    });
    
    closeModalBtn.addEventListener('click', () => {
        dbInfoModal.classList.add('hidden');
    });
    
    // Close modal when clicking outside
    dbInfoModal.addEventListener('click', (e) => {
        if (e.target === dbInfoModal) {
            dbInfoModal.classList.add('hidden');
        }
    });
    
    // Initialize WebSocket connection
    connectWebSocket();
});