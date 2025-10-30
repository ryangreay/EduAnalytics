import os
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone
from langchain.tools import Tool
from langchain_core.documents import Document

class EntityResolver:
    """Pinecone-based entity resolver for high-cardinality columns"""
    
    def __init__(self):
        # Read Pinecone API key
        pinecone_key = os.getenv("PINECONE_API_KEY")
        if not pinecone_key:
            try:
                with open("pinecone_api_key.txt", "r") as f:
                    pinecone_key = f.read().strip()
            except:
                pinecone_key = None
        
        self.enabled = pinecone_key is not None
        
        if self.enabled:
            index_name = os.getenv("PINECONE_INDEX_NAME", "eduanalytics-entities")
            self.embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
            self.vector_store = PineconeVectorStore(
                index_name=index_name, 
                embedding=self.embeddings
            )
    
    def search(self, text: str, k=5):
        """Search for similar entities"""
        if not self.enabled:
            return []
        
        results = self.vector_store.similarity_search(text, k=k)
        return [doc.metadata for doc in results]
    
    def search_as_text(self, text: str, k=5) -> str:
        """Search and format results as text for the agent"""
        if not self.enabled:
            return "Entity search is not available."
        
        results = self.vector_store.similarity_search(text, k=k)
        
        if not results:
            return "No matching entities found."
        
        output_lines = []
        for i, doc in enumerate(results, 1):
            meta = doc.metadata
            entity_type = meta.get("type", "unknown")
            
            if entity_type == "entity":
                label = f"{meta.get('county_name', '')} | {meta.get('district_name', '')} | {meta.get('school_name', '')}".strip(" |")
                output_lines.append(f"{i}. {label} (County: {meta.get('county_code', 'N/A')} District: {meta.get('district_code', 'N/A')} School: {meta.get('school_code', 'N/A')})")
            elif entity_type == "subgroup":
                output_lines.append(f"{i}. {meta.get('demographic_name', '')} (ID: {meta.get('demographic_id', '')})")
            elif entity_type == "test":
                output_lines.append(f"{i}. {meta.get('test_name', '')} (ID: {meta.get('test_id', '')})")
            elif entity_type == "grade":
                output_lines.append(f"{i}. Grade {meta.get('grade', '')}")
        
        return "\n".join(output_lines)
    
    def as_tool(self) -> Tool:
        """Create a LangChain Tool for the agent"""
        return Tool(
            name="search_proper_nouns",
            description=(
                "Use this tool to look up proper nouns and identifiers before filtering data. "
                "Input should be an approximate spelling of a county, district, school, student subgroup, "
                "grade, or test name. Output will be the correct names and IDs to use in SQL queries. "
                "Each entity has a combination of the county code, district code, and school code and can be used to filter on the same fields in the fact_scores table."
                "ALWAYS use this tool before filtering on entity names, subject/test subgroups, or grades."
            ),
            func=self.search_as_text
        )
