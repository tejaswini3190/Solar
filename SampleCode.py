import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import pandas as pd
from datetime import datetime

# MCP functionality to be implemented when MCP packages are available
# TODO: Add MCP imports when packages are ready

# Azure Data Explorer imports
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

# SharePoint imports
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataSource(Enum):
    ADX = "adx"
    SHAREPOINT = "sharepoint"
    DATABASE = "database"

@dataclass
class QueryResult:
    source: DataSource
    data: pd.DataFrame
    sample_size: int
    total_rows: int
    query: str
    timestamp: datetime

class DataSourceConfig:
    """Configuration for different data sources"""
    
    def __init__(self):
        self.adx_config = {
            "cluster_url": "https://sembsolar-adx.kusto.windows.net",  # Replace with your ADX cluster
            "database": "sembsolar_db",  # Replace with your database name
            "client_id": "your-client-id",  # Replace with your Azure AD app client ID
            "client_secret": "your-client-secret",  # Replace with your client secret
            "tenant_id": "your-tenant-id"  # Replace with your Azure AD tenant ID
        }
        
        self.sharepoint_config = {
            "site_url": "https://sembcorp.sharepoint.com/sites/sembsolar",  # Replace with your SharePoint site
            "username": "your-username@sembcorp.com",  # Replace with your email
            "password": "your-password"  # Consider using secure credential storage
        }

class ADXConnector:
    """Azure Data Explorer connector"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.client = None
        self._connect()
    
    def _connect(self):
        """Establish connection to ADX"""
        try:
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                self.config["cluster_url"],
                self.config["client_id"],
                self.config["client_secret"],
                self.config["tenant_id"]
            )
            self.client = KustoClient(kcsb)
            logger.info("Connected to Azure Data Explorer")
        except Exception as e:
            logger.error(f"Failed to connect to ADX: {e}")
            raise
    
    def execute_query(self, query: str, sample_only: bool = True, sample_size: int = 100) -> QueryResult:
        """Execute KQL query and return results"""
        try:
            if sample_only:
                # Modify query to include LIMIT for sample
                if "| limit" not in query.lower():
                    query = f"{query} | limit {sample_size}"
            
            response = self.client.execute(self.config["database"], query)
            
            # Convert to DataFrame
            df = pd.DataFrame(response.primary_results[0])
            
            # Get total count (you might need a separate count query)
            count_query = f"({query.split('| limit')[0] if '| limit' in query else query}) | count"
            count_response = self.client.execute(self.config["database"], count_query)
            total_rows = count_response.primary_results[0][0]['Count'] if count_response.primary_results[0] else len(df)
            
            return QueryResult(
                source=DataSource.ADX,
                data=df,
                sample_size=len(df),
                total_rows=total_rows,
                query=query,
                timestamp=datetime.utcnow()
            )
        except KustoServiceError as e:
            logger.error(f"ADX query failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in ADX query: {e}")
            raise

class SharePointConnector:
    """SharePoint connector"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.ctx = None
        self._connect()
    
    def _connect(self):
        """Establish connection to SharePoint"""
        try:
            auth_ctx = AuthenticationContext(self.config["site_url"])
            auth_ctx.acquire_token_for_user(self.config["username"], self.config["password"])
            self.ctx = ClientContext(self.config["site_url"], auth_ctx)
            logger.info("Connected to SharePoint")
        except Exception as e:
            logger.error(f"Failed to connect to SharePoint: {e}")
            raise
    
    def get_file_data(self, file_path: str, sample_only: bool = True, sample_size: int = 100) -> QueryResult:
        """Get data from SharePoint file"""
        try:
            # Download file
            file_response = File.open_binary(self.ctx, file_path)
            
            # Assuming it's an Excel or CSV file
            if file_path.endswith('.xlsx'):
                df = pd.read_excel(file_response.content)
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_response.content)
            else:
                raise ValueError(f"Unsupported file type: {file_path}")
            
            total_rows = len(df)
            
            if sample_only:
                df = df.head(sample_size)
            
            return QueryResult(
                source=DataSource.SHAREPOINT,
                data=df,
                sample_size=len(df),
                total_rows=total_rows,
                query=f"SharePoint file: {file_path}",
                timestamp=datetime.utcnow()
            )
        except Exception as e:
            logger.error(f"SharePoint file access failed: {e}")
            raise

class MCPChatbot:
    """Main chatbot class with MCP integration"""
    
    def __init__(self):
        self.config = DataSourceConfig()
        self.adx_connector = ADXConnector(self.config.adx_config)
        self.sharepoint_connector = SharePointConnector(self.config.sharepoint_config)
        self.conversation_history = []
        self.last_query_result = None
        
    async def initialize_mcp(self):
        """Initialize MCP client - TODO: Implement when MCP packages are available"""
        # TODO: Implement MCP initialization when packages are installed
        logger.info("MCP initialization placeholder - implement when ready")
        pass
    
    def parse_user_query(self, user_input: str) -> Dict[str, Any]:
        """Parse user input to determine intent and extract parameters"""
        user_input_lower = user_input.lower()
        
        # Simple intent detection (you can enhance this with NLP)
        if any(keyword in user_input_lower for keyword in ["adx", "kusto", "azure data explorer"]):
            return {
                "intent": "query_adx",
                "source": DataSource.ADX,
                "query": user_input
            }
        elif any(keyword in user_input_lower for keyword in ["sharepoint", "excel", "csv", "file"]):
            return {
                "intent": "query_sharepoint",
                "source": DataSource.SHAREPOINT,
                "query": user_input
            }
        elif any(keyword in user_input_lower for keyword in ["download", "full data", "complete data"]):
            return {
                "intent": "download_full_data",
                "query": user_input
            }
        elif any(keyword in user_input_lower for keyword in ["sample", "preview", "show me"]):
            return {
                "intent": "show_sample",
                "query": user_input
            }
        else:
            return {
                "intent": "general_query",
                "query": user_input
            }
    
    def generate_sample_data(self, source: DataSource) -> QueryResult:
        """Generate sample data for demonstration"""
        if source == DataSource.ADX:
            # Sample ADX data
            sample_data = pd.DataFrame({
                'timestamp': pd.date_range('2025-01-01', periods=10, freq='H'),
                'user_id': range(1, 11),
                'event_type': ['login', 'logout', 'click', 'purchase'] * 2 + ['view', 'search'],
                'value': [100, 200, 150, 300, 250, 180, 120, 400, 350, 220]
            })
            return QueryResult(
                source=DataSource.ADX,
                data=sample_data,
                sample_size=10,
                total_rows=10000,  # Simulated total
                query="Sample ADX query: Events | where timestamp > ago(1d)",
                timestamp=datetime.utcnow()
            )
        
        elif source == DataSource.SHAREPOINT:
            # Sample SharePoint data
            sample_data = pd.DataFrame({
                'document_id': range(1, 11),
                'title': [f'Document {i}' for i in range(1, 11)],
                'author': ['Alice', 'Bob', 'Charlie'] * 3 + ['Diana'],
                'created_date': pd.date_range('2025-01-01', periods=10, freq='D'),
                'size_mb': [1.2, 2.5, 0.8, 3.1, 1.9, 2.3, 1.5, 2.8, 1.1, 2.0]
            })
            return QueryResult(
                source=DataSource.SHAREPOINT,
                data=sample_data,
                sample_size=10,
                total_rows=5000,  # Simulated total
                query="Sample SharePoint query: Documents from Q1 2025",
                timestamp=datetime.utcnow()
            )
    
    def format_result_for_user(self, result: QueryResult) -> str:
        """Format query result for user display"""
        response = f"""
📊 **Data from {result.source.value.upper()}**

🔍 **Query:** {result.query}
📅 **Timestamp:** {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC
📈 **Sample Size:** {result.sample_size} rows
📊 **Total Available:** {result.total_rows} rows

**Sample Data Preview:**