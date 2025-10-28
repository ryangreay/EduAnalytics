import json, os, re
from sqlalchemy import create_engine, text
from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import (
    InfoSQLDatabaseTool,
    ListSQLDatabaseTool,
    QuerySQLDataBaseTool,
)
from langchain.tools import Tool

class SQLToolkit:
    """Toolkit for SQL database operations with LangChain agent"""
    
    def __init__(self, pg_url: str, whitelist_path: str):
        self.engine = create_engine(pg_url, pool_pre_ping=True)
        self.last_query_text = None
        self.last_error_text = None

        # Load schema whitelist
        with open(whitelist_path, "r") as f:
            self.schema = json.load(f)

        # Keep whitelist table keys as fully-qualified names
        self.table_keys = list(self.schema.get("tables", {}).keys())

        # Create SQLDatabase instance without setting schema (avoids SET search_path issues)
        self.db = SQLDatabase.from_uri(
            pg_url,
            sample_rows_in_table_info=3,
        )
        
        # Track SQL query errors for retry limit
        self.error_count = 0
        self.max_attempts = 3
    
    def get_tools(self):
        """Return list of SQL tools for the agent (without retry limit)"""
        
        # List tables tool (from whitelist)
        def list_tables_impl(_: str = "") -> str:
            return ", ".join(self.table_keys)

        list_tables_tool = Tool(
            name="sql_db_list_tables",
            description="List available tables (from whitelist).",
            func=list_tables_impl,
        )

        # Get schema tool (from whitelist)
        def schema_info_impl(_: str = "") -> str:
            parts = []
            for tbl, cols in self.schema.get("tables", {}).items():
                parts.append(f"Table: {tbl}\nColumns: {', '.join(cols)}")
            return "\n\n".join(parts)

        get_schema_tool = Tool(
            name="sql_db_schema",
            description=(
                "Get table schemas (from whitelist). Use fully-qualified names like analytics.fact_scores."
            ),
            func=schema_info_impl,
        )
        
        # Query tool with safety wrapper
        def safe_query_wrapper(query: str) -> str:
            """Execute SQL query with safety checks"""
            # Safety check - block non-SELECT queries
            lower = query.lower().strip()
            if any(x in lower for x in ["insert", "update", "delete", "drop", "alter", "create", "truncate"]):
                return "Error: Only SELECT queries are allowed."
            
            try:
                self.last_query_text = query
                result = self.db.run(query)
                self.last_error_text = None
                return result
            except Exception as e:
                self.last_error_text = str(e)
                return f"Error executing query: {str(e)}"
        
        query_tool = Tool(
            name="sql_db_query",
            description=(
                "Execute a SQL query against the database and get results. "
                "Input should be a valid SQL SELECT query. "
                "\n\n"
                "⚠️ CRITICAL: Each row is a PRE-AGGREGATED statistic. You MUST filter on ALL dimensions:\n"
                "   • cds_code: If NO entity mentioned → '00000000000000' (statewide). If mentioned → call search_proper_nouns\n"
                "   • subgroup: If NO subgroup mentioned → '1' (all students). If mentioned → call search_proper_nouns\n"
                "   • grade: If NO grade mentioned → '13' (all grades). If mentioned → call search_proper_nouns\n"
                "   • test_id: If NO test mentioned → IN ('1','2') + GROUP BY test_id. If 'ELA' → '1', if 'Math' → '2'\n"
                "\n"
                "Other Rules:\n"
                "- Only SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)\n"
                "- ALWAYS use fully-qualified table names like analytics.fact_scores\n"
                "- Use COALESCE(column, 0) in ALL aggregations and in ORDER BY for rankings\n"
                "- For 'latest year', use MAX(year_key) or ORDER BY year_key DESC LIMIT 1\n"
            ),
            func=safe_query_wrapper
        )
        
        return [list_tables_tool, get_schema_tool, query_tool]
    
    def get_tools_with_retry_limit(self, max_attempts: int = 3):
        """Return list of SQL tools with retry limit for query errors"""
        
        self.max_attempts = max_attempts
        self.error_count = 0
        self.last_query_succeeded = True
        
        # List tables tool (from whitelist)
        def list_tables_impl(_: str = "") -> str:
            return ", ".join(self.table_keys)

        list_tables_tool = Tool(
            name="sql_db_list_tables",
            description="List available tables (from whitelist).",
            func=list_tables_impl,
        )

        # Get schema tool (from whitelist)
        def schema_info_impl(_: str = "") -> str:
            parts = []
            for tbl, cols in self.schema.get("tables", {}).items():
                parts.append(f"Table: {tbl}\nColumns: {', '.join(cols)}")
            return "\n\n".join(parts)

        get_schema_tool = Tool(
            name="sql_db_schema",
            description=(
                "Get table schemas (from whitelist). Use fully-qualified names like analytics.fact_scores."
            ),
            func=schema_info_impl,
        )
        
        # Query tool with safety wrapper and retry limit
        def safe_query_with_retry_limit(query: str) -> str:
            """Execute SQL query with safety checks and retry limit"""
            # Safety check - block non-SELECT queries
            lower = query.lower().strip()
            if any(x in lower for x in ["insert", "update", "delete", "drop", "alter", "create", "truncate"]):
                return "Error: Only SELECT queries are allowed."
            
            # Check if we've exceeded retry limit
            if self.error_count >= self.max_attempts:
                return (
                    f"Error: Maximum query attempts ({self.max_attempts}) exceeded. "
                    "I've tried multiple times but cannot generate a working query. "
                    "Please try rephrasing your question or ask something else."
                )
            
            try:
                self.last_query_text = query
                result = self.db.run(query)
                # Reset error count on success
                self.error_count = 0
                self.last_query_succeeded = True
                self.last_error_text = None
                return result
            except Exception as e:
                # Increment error count
                self.error_count += 1
                self.last_query_succeeded = False
                
                self.last_error_text = str(e)
                error_msg = f"Error executing query (attempt {self.error_count}/{self.max_attempts}): {str(e)}"
                
                if self.error_count >= self.max_attempts:
                    error_msg += (
                        "\n\nMaximum retry attempts reached. Please try rephrasing your question "
                        "or provide more specific details about what you're looking for."
                    )
                
                return error_msg
        
        query_tool = Tool(
            name="sql_db_query",
            description=(
                "Execute a SQL query against the database and get results. "
                "Input should be a valid SQL SELECT query. "
                "\n\n"
                "⚠️ CRITICAL: Each row is a PRE-AGGREGATED statistic. You MUST filter on ALL dimensions:\n"
                "   • cds_code: If NO entity mentioned → '00000000000000' (statewide). If mentioned → call search_proper_nouns\n"
                "   • subgroup: If NO subgroup mentioned → '1' (all students). If mentioned → call search_proper_nouns\n"
                "   • grade: If NO grade mentioned → '13' (all grades). If mentioned → call search_proper_nouns\n"
                "   • test_id: If NO test mentioned → IN ('1','2') + GROUP BY test_id. If 'ELA' → '1', if 'Math' → '2'\n"
                "\n"
                "Other Rules:\n"
                "- Only SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)\n"
                "- ALWAYS use fully-qualified table names like analytics.fact_scores\n"
                "- Use COALESCE(column, 0) in ALL aggregations and in ORDER BY for rankings\n"
                "- For 'latest year', use MAX(year_key) or ORDER BY year_key DESC LIMIT 1\n"
                f"- You have up to {max_attempts} attempts to fix errors before giving up\n"
            ),
            func=safe_query_with_retry_limit
        )
        
        return [list_tables_tool, get_schema_tool, query_tool]
    
    def reset_error_count(self):
        """Reset the error count for a new question"""
        self.error_count = 0
        self.last_query_succeeded = True
    
    def get_table_info(self) -> str:
        """Get information about all tables in the database"""
        return self.db.get_table_info()
    
    def run_query(self, query: str):
        """Execute a query and return results as list of dicts"""
        lower = query.lower().strip()
        if any(x in lower for x in ["insert", "update", "delete", "drop", "alter", "create", "truncate"]):
            return []
        
        try:
            with self.engine.begin() as con:
                rows = con.execute(text(query)).fetchall()
                return [dict(r._mapping) for r in rows]
        except Exception as e:
            print(f"Error executing query: {e}")
            return []
