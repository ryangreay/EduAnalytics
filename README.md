# EduAnalytics - CAASPP Education Data AI Assistant

An AI-powered conversational assistant for analyzing California Assessment of Student Performance and Progress (CAASPP) data using Prefect for ETL, LangGraph ReAct agents, SQL database queries, and Pinecone vector embeddings for entity resolution. The chat aspect is built using Streamlit and hosted through fly.io at [https://cdeanalytics.fly.dev](https://cdeanalytics.fly.dev).

## Features

- **LangGraph ReAct Agent**: Sophisticated conversational AI that can reason about queries and use tools to find answers
- **Entity Resolution**: Uses Pinecone vector embeddings to lookup proper nouns (counties, districts, schools, student subgroups, grades)
- **SQL Query Generation**: Intelligent SQL query generation with schema awareness
- **Data Visualization**: Automatic chart generation for query results
- **Conversational Interface**: Natural language question answering with context awareness
- **Automated Data Ingestion**: Prefect flows to automatically fetch and process data from California Department of Education

## Architecture

The system follows the [LangChain SQL Tutorial](https://python.langchain.com/docs/tutorials/sql_qa/) architecture with:

1. **ReAct Agent**: Uses reasoning and acting loops to answer questions
2. **SQL Tools**: `sql_db_list_tables`, `sql_db_schema`, `sql_db_query`
3. **Entity Lookup Tool**: `search_proper_nouns` - searches Pinecone for high-cardinality entities
4. **Prefect**: ETL ingestion flow to go from CDE website to database
5. **PostgreSQL Database**: Stores test scores, proficiency data, and metadata
6. **Pinecone Vector Store**: Stores embeddings for counties, districts, schools, subgroups, tests, and grades

## Usage

### Example Questions

The AI assistant can answer questions like:

- "Show top 10 districts by Math proficiency in the latest year"
- "What is the average ELA proficiency for Hispanic students in grade 5?"
- "Compare Math scores for English learners vs. all students in 2023"
- "Which schools in Los Angeles Unified have the highest proficiency?"
- "Show ELA trends for socioeconomically disadvantaged students over the last 3 years"
- "What percentage of grade 8 students met or exceeded standards in Math?"
- "What is the proficiency band breakdown for for ELA in the latest year at El Dorado County?"

### How It Works

1. **User asks a question** in natural language
2. **Agent analyzes** the question and determines what tools to use. One of the most important aspects of this project is providing a well worded and structured system prompt to highlight most important aspects like how to query the data, build charts, and do embedding lookups. This involves including lots of SQL examples and structured text with emojis, CAPS, and bulleted lists.
3. **Entity lookup** (if needed): Searches Pinecone for proper nouns like "Los Angeles Unified" or "Hispanic students"
4. **Schema inspection**: Gets table structures using `sql_db_schema`
5. **SQL generation**: Creates appropriate SELECT query
6. **Query execution**: Runs the query and returns results
7. **Response**: Formats and presents the answer with visualizations by requesting structured JSON chart output and chart type recommendation, then building with plotly.

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
- `test_id` (subject): ELA = 1 / Math = 2
- `subgroup`: Student demographic group
- `grade`: Grade level (3-8, 11, All)
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
│   ├── schema_whitelist.json # DB schema configuration
│   └── sql_examples.json     # SQL query examples
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
- **Example**: "LA Unified" -> searches Pinecone -> finds "Los Angeles Unified School District"

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
- Agent does not expose schema information in chat when asked

## Data Sources

Data is fetched from the [California Department of Education CAASPP Research Files](https://caaspp-elpac.ets.org/caaspp/ResearchFileListSB):

- **Test Results**: Smarter Balanced assessment results by entity, grade, subgroup
- **Entities**: Counties, districts, schools with CDS codes
- **StudentGroups**: Demographic group definitions and IDs
- **Tests**: Test type definitions and IDs

## Future Improvement

- **Update to `create_agent`**: The latest version of langchain/langgraph are deprecating the `create_react_agent()`.
- **Improved agent accuracy/performance tracking**: Integrating Langsmith for more detailed agent monitoring and evaluation.
- **Improved schema and additional data sources**: Currently we just have 2 tables and we could also break out the subgroups, grades, tests, and entities into their own respective tables. If we added more CDE data sources such as, entity enrollment, student absenteeism, and discipline, breaking schema out into multiple table relationships would be much more important. 

## References

- [LangChain SQL QA Tutorial](https://python.langchain.com/docs/tutorials/sql_qa/)
- [LangGraph Documentation](https://langchain-ai.github.io/langgraph/)
- [California CAASPP Data](https://caaspp-elpac.ets.org/)
- [Prefect Documentation](https://docs.prefect.io/v3/how-to-guides)
- [Pinecone Documentation](https://docs.pinecone.io/integrations/langchain)

