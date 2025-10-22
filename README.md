# EduAnalytics - CAASPP Education Data AI Assistant

An AI-powered conversational assistant for analyzing California Assessment of Student Performance and Progress (CAASPP) data using LangGraph ReAct agents, SQL database queries, and Pinecone vector embeddings for entity resolution.

## Features

- 🤖 **LangGraph ReAct Agent**: Sophisticated conversational AI that can reason about queries and use tools to find answers
- 🔍 **Entity Resolution**: Uses Pinecone vector embeddings to lookup proper nouns (counties, districts, schools, student subgroups, grades)
- 🗄️ **SQL Query Generation**: Intelligent SQL query generation with schema awareness
- 📊 **Data Visualization**: Automatic chart generation for query results
- 💬 **Conversational Interface**: Natural language question answering with context awareness
- 🔄 **Automated Data Ingestion**: Prefect flows to automatically fetch and process data from California Department of Education

## Architecture

The system follows the [LangChain SQL Tutorial](https://python.langchain.com/docs/tutorials/sql_qa/) architecture with:

1. **ReAct Agent**: Uses reasoning and acting loops to answer questions
2. **SQL Tools**: `sql_db_list_tables`, `sql_db_schema`, `sql_db_query`
3. **Entity Lookup Tool**: `search_proper_nouns` - searches Pinecone for high-cardinality entities
4. **PostgreSQL Database**: Stores test scores, proficiency data, and metadata
5. **Pinecone Vector Store**: Stores embeddings for counties, districts, schools, subgroups, tests, and grades

## Setup

### Prerequisites

- Python 3.10+
- PostgreSQL database
- Pinecone account (free tier works)
- OpenAI API key

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Environment Variables

Create a `.env` file in the project root:

```env
# PostgreSQL
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=eduanalytics

# OpenAI
OPENAI_API_KEY=your_openai_key

# Pinecone
PINECONE_API_KEY=your_pinecone_key
PINECONE_INDEX_NAME=eduanalytics-entities

# Data ingestion settings
LAST_YEARS=3
DATA_DIR=./data
```

Alternatively, for Pinecone, you can create a `pinecone_api_key.txt` file with your API key.

### 3. Initialize Database

Run the DDL script to create tables:

```bash
psql -U your_user -d eduanalytics -f db/ddl.sql
```

### 4. Ingest Data

Run the Prefect flow to fetch data from California Department of Education:

```bash
python -m ingest.flow
```

This will:
- Fetch CAASPP test results for the last 3 years
- Download entity files (counties, districts, schools)
- Download StudentGroups and Tests reference files
- Load data into PostgreSQL
- Create Pinecone index with embeddings for:
  - Counties, districts, and schools
  - Student demographic subgroups
  - Test types
  - Grade levels

The Pinecone index will be created automatically with 3072 dimensions (for OpenAI `text-embedding-3-large`).

### 5. Run the Streamlit App

```bash
streamlit run ui/streamlit_app.py
```

Access the app at `http://localhost:8501`

## Usage

### Example Questions

The AI assistant can answer questions like:

- "Show top 10 districts by Math proficiency in the latest year"
- "What is the average ELA proficiency for Hispanic students in grade 5?"
- "Compare Math scores for English learners vs. all students in 2023"
- "Which schools in Los Angeles Unified have the highest proficiency?"
- "Show ELA trends for socioeconomically disadvantaged students over the last 3 years"
- "What percentage of grade 8 students met or exceeded standards in Math?"

### How It Works

1. **User asks a question** in natural language
2. **Agent analyzes** the question and determines what tools to use
3. **Entity lookup** (if needed): Searches Pinecone for proper nouns like "Los Angeles Unified" or "Hispanic students"
4. **Schema inspection**: Gets table structures using `sql_db_schema`
5. **SQL generation**: Creates appropriate SELECT query
6. **Query execution**: Runs the query and returns results
7. **Response**: Formats and presents the answer with visualizations

### Agent Tools

The agent has access to these tools:

- `sql_db_list_tables`: List available database tables
- `sql_db_schema`: Get schema information for specific tables
- `sql_db_query`: Execute SQL SELECT queries
- `search_proper_nouns`: Look up entities, subgroups, grades, etc. in Pinecone

## Data Structure

### PostgreSQL Tables

**analytics.fact_scores**
- `year_key`: Academic year (e.g., 2023)
- `subject`: Math or ELA
- `subgroup`: Student demographic group
- `grade`: Grade level (3-8, 11)
- `district_name`, `school_name`: Entity names
- `tested`, `tested_with_scores`: Student counts
- `mean_scale_score`: Average scale score
- `pct_met_and_above`: Proficiency percentage
- Other proficiency breakdowns (exceeded, met, nearly met, not met)

**analytics.dim_year**
- `year_key`: Academic year
- `label`: Display label (e.g., "AY 2023-24")

### Pinecone Vector Store

Entities are stored with metadata:

**Entity Types:**
- `entity`: Counties, districts, schools (with CDS codes)
- `subgroup`: Student demographic groups (with IDs)
- `test`: Test types (SB-ELA, SB-Math, CAA, CAST, CSA)
- `grade`: Grade levels (3-8, 11)

## Project Structure

```
EduAnalytics/
├── app/
│   ├── agent.py              # LangGraph ReAct agent
│   ├── tools_sql.py          # SQL toolkit
│   ├── tools_entity.py       # Pinecone entity resolver
│   ├── prompts.py            # System prompts
│   ├── chart_rules.py        # Chart selection logic
│   └── schema_whitelist.json # DB schema configuration
├── db/
│   └── ddl.sql               # Database schema
├── ingest/
│   ├── flow.py               # Prefect data ingestion flow
│   ├── config.py             # Configuration
│   └── transforms.py         # Data transformations
├── ui/
│   └── streamlit_app.py      # Streamlit web interface
├── sample_data/              # Sample reference files
├── requirements.txt
└── README.md
```

## Key Features Explained

### High-Cardinality Entity Lookup

The system uses Pinecone embeddings to handle high-cardinality columns (entities with many unique values):

- **Problem**: District names, school names, subgroups can be misspelled or ambiguous
- **Solution**: Agent uses `search_proper_nouns` tool to find correct spelling/ID before querying
- **Example**: "LA Unified" → searches Pinecone → finds "Los Angeles Unified School District"

### ReAct Agent Pattern

The agent follows a reasoning loop:

1. **Thought**: Analyze what information is needed
2. **Action**: Call appropriate tool (entity lookup, schema check, SQL query)
3. **Observation**: Process tool result
4. **Repeat** until question is answered

### Safety Features

- SQL queries are restricted to SELECT only
- Schema whitelist limits accessible tables
- Query validation before execution
- Error handling with helpful messages

## Troubleshooting

### Pinecone Index Not Found

If you see errors about missing Pinecone index:
1. Ensure `PINECONE_API_KEY` is set correctly
2. Run the ingest flow to create the index: `python -m ingest.flow`
3. Check Pinecone dashboard to verify index exists

### Database Connection Issues

- Verify PostgreSQL is running
- Check `.env` file has correct credentials
- Ensure database exists: `createdb eduanalytics`

### OpenAI Rate Limits

If you hit rate limits:
- The system uses `gpt-4o` for the agent (can change to `gpt-4o-mini` in `app/agent.py`)
- Embeddings use `text-embedding-3-large`

## Data Sources

Data is fetched from the [California Department of Education CAASPP Research Files](https://caaspp-elpac.ets.org/caaspp/ResearchFileListSB):

- **Test Results**: Smarter Balanced assessment results by entity, grade, subgroup
- **Entities**: Counties, districts, schools with CDS codes
- **StudentGroups**: Demographic group definitions and IDs
- **Tests**: Test type definitions and IDs

## Contributing

To extend the system:

1. **Add new tools**: Create tool functions in `app/tools_*.py` and add to agent
2. **Modify prompts**: Edit `app/prompts.py` to change agent behavior
3. **Add data sources**: Update `ingest/flow.py` to pull additional data
4. **Enhance UI**: Modify `ui/streamlit_app.py` for better visualizations

## License

MIT License - See LICENSE file for details

## References

- [LangChain SQL QA Tutorial](https://python.langchain.com/docs/tutorials/sql_qa/)
- [LangGraph Documentation](https://langchain-ai.github.io/langgraph/)
- [California CAASPP Data](https://caaspp-elpac.ets.org/)
- [Pinecone Documentation](https://docs.pinecone.io/)

