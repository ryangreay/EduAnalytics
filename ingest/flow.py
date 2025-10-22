import os, re, datetime, json, zipfile, io
import requests, polars as pl, numpy as np
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pinecone import Pinecone, ServerlessSpec
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from .config import DATA_DIR, CAASPP_LIST, STUDENT_GROUPS_URL, TESTS_URL
from .transforms import parse_zip_caret, parse_student_groups, parse_tests

load_dotenv()
PG_URL = (
    f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

@task(retries=2, retry_delay_seconds=5)
def http_get(url: str) -> bytes:
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    return r.content

@task
def caret_zip_urls(list_html: bytes) -> list[str]:
    hrefs = re.findall(rb'href="([^"]+\.zip)"', list_html, re.IGNORECASE)
    out = []
    for h in hrefs:
        u = h.decode("utf-8")
        if u.startswith("/"):
            u = "https://caaspp-elpac.ets.org" + u
        if "caret" in u.lower() or "csv" in u.lower():
            out.append(u)
    return list(dict.fromkeys(out))

@task
def ensure_years(engine, latest_year: int, last_years: int):
    with engine.begin() as con:
        for y in range(latest_year - last_years + 1, latest_year + 1):
            con.execute(text("""
                INSERT INTO analytics.dim_year(year_key,label)
                VALUES(:y,:l)
                ON CONFLICT (year_key) DO NOTHING
            """), {"y": y, "l": f"AY {y}-{y+1}"})

@task
def load_tests(engine, tests: pl.DataFrame, year_key: int):
    if tests is None or tests.height == 0:
        return
    ren = {
        "student_group_id":"subgroup",
        "grade":"grade",
        "total_students_tested":"tested",
        "total_students_tested_with_scores":"tested_with_scores",
        "mean_scale_score":"mean_scale_score",
        "percentage_standard_exceeded":"pct_exceeded",
        "count_standard_exceeded":"cnt_exceeded",
        "percentage_standard_met":"pct_met",
        "count_standard_met":"cnt_met",
        "percentage_standard_met_and_above":"pct_met_and_above",
        "count_standard_met_and_above":"cnt_met_and_above",
        "percentage_standard_nearly_met":"pct_nearly_met",
        "count_standard_nearly_met":"cnt_nearly_met",
        "percentage_standard_not_met":"pct_not_met",
        "count_standard_not_met":"cnt_not_met",
        "district_name":"district_name",
        "school_name":"school_name",
        "test_id":"test_id"
    }
    cols = [c for c in ren if c in tests.columns]
    df = tests.select(cols).rename({c: ren[c] for c in cols})
    # Subject heuristic: test_id 1 = ELA, 2 = Math (adjust if your files label differently)
    df = df.with_columns([
        pl.when(pl.col("test_id")==2).then("Math").otherwise("ELA").alias("subject"),
        pl.lit(year_key).alias("year_key")
    ]).drop(["test_id"])
    with engine.begin() as con:
        con.execute(text("CREATE TEMP TABLE tmp_scores AS SELECT * FROM analytics.fact_scores WITH NO DATA"))
        df.to_pandas().to_sql("tmp_scores", con.connection, if_exists="append", index=False)
        con.execute(text("""
        INSERT INTO analytics.fact_scores(
          year_key, subject, subgroup, grade,
          tested, tested_with_scores, mean_scale_score,
          pct_exceeded, cnt_exceeded, pct_met, cnt_met,
          pct_met_and_above, cnt_met_and_above, pct_nearly_met, cnt_nearly_met,
          pct_not_met, cnt_not_met, district_name, school_name
        )
        SELECT
          year_key, subject, subgroup::text, grade::text,
          tested::int, tested_with_scores::int, mean_scale_score::numeric,
          pct_exceeded::numeric, cnt_exceeded::int, pct_met::numeric, cnt_met::int,
          pct_met_and_above::numeric, cnt_met_and_above::int, pct_nearly_met::numeric, cnt_nearly_met::int,
          pct_not_met::numeric, cnt_not_met::int, district_name::text, school_name::text
        FROM tmp_scores
        """))

@task
def build_pinecone_index(pairs: list[tuple[int, bytes]], student_groups_df: pl.DataFrame, tests_df: pl.DataFrame):
    """Build Pinecone index with entities, student groups, tests, grades"""
    logger = get_run_logger()
    
    # Read Pinecone API key
    pinecone_key = os.getenv("PINECONE_API_KEY")
    if not pinecone_key:
        with open("pinecone_api_key.txt", "r") as f:
            pinecone_key = f.read().strip()
    
    # Initialize Pinecone
    pc = Pinecone(api_key=pinecone_key)
    index_name = os.getenv("PINECONE_INDEX_NAME", "eduanalytics-entities")
    
    # Create index if it doesn't exist
    existing_indexes = [idx.name for idx in pc.list_indexes()]
    if index_name not in existing_indexes:
        logger.info(f"Creating Pinecone index: {index_name}")
        pc.create_index(
            name=index_name,
            dimension=3072,  # text-embedding-3-large dimension
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1")
        )
    
    # Initialize embeddings and vector store
    embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
    vector_store = PineconeVectorStore(index_name=index_name, embedding=embeddings)
    
    texts, metadatas = [], []
    
    # 1. Add entities (counties, districts, schools)
    for year_key, zip_bytes in pairs:
        parts = parse_zip_caret(zip_bytes)
        ents = parts["entities"]
        if ents is None or ents.height == 0:
            continue
        keep = [c for c in ["county_code","district_code","school_code","type_id","test_year","county_name","district_name","school_name","zip_code"] if c in ents.columns]
        ents = ents.select(keep).with_columns(pl.lit(year_key).alias("year_key"))
        
        for r in ents.iter_rows(named=True):
            county_name = str(r.get("county_name") or "").strip()
            district_name = str(r.get("district_name") or "").strip()
            school_name = str(r.get("school_name") or "").strip()
            
            label = " | ".join([county_name, district_name, school_name]).strip(" |")
            code = f"{r.get('county_code',''):0>2}{r.get('district_code',''):0>5}{r.get('school_code',''):0>7}"
            
            text = f"{label} | CDS:{code} | type:{r.get('type_id','')}"
            texts.append(text)
            metadatas.append({
                "type": "entity",
                "county_name": county_name,
                "district_name": district_name,
                "school_name": school_name,
                "county_code": str(r.get("county_code", "")),
                "district_code": str(r.get("district_code", "")),
                "school_code": str(r.get("school_code", "")),
                "cds_code": code,
                "year_key": year_key
            })
    
    # 2. Add student groups/subgroups
    if student_groups_df is not None and student_groups_df.height > 0:
        for r in student_groups_df.iter_rows(named=True):
            demo_name = str(r.get("demographic_name", ""))
            demo_id = str(r.get("demographic_id_num", ""))
            student_group = str(r.get("student_group", ""))
            
            text = f"{demo_name} (Subgroup ID: {demo_id}, Category: {student_group})"
            texts.append(text)
            metadatas.append({
                "type": "subgroup",
                "demographic_name": demo_name,
                "demographic_id": demo_id,
                "student_group": student_group
            })
    
    # 3. Add tests
    if tests_df is not None and tests_df.height > 0:
        for r in tests_df.iter_rows(named=True):
            test_name = str(r.get("test_name", ""))
            test_id = str(r.get("test_id_num", ""))
            
            text = f"{test_name} (Test ID: {test_id})"
            texts.append(text)
            metadatas.append({
                "type": "test",
                "test_name": test_name,
                "test_id": test_id
            })
    
    # 4. Add grades
    grades = ["3", "4", "5", "6", "7", "8", "11", "Grade 3", "Grade 4", "Grade 5", "Grade 6", "Grade 7", "Grade 8", "Grade 11"]
    for grade in grades:
        texts.append(f"Grade {grade}")
        metadatas.append({
            "type": "grade",
            "grade": grade
        })
    
    # Upload to Pinecone in batches
    if texts:
        logger.info(f"Uploading {len(texts)} entities to Pinecone index: {index_name}")
        # Clear existing data first
        pc.Index(index_name).delete(delete_all=True)
        
        # Add in batches
        batch_size = 100
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i+batch_size]
            batch_metas = metadatas[i:i+batch_size]
            vector_store.add_texts(batch_texts, metadatas=batch_metas)
            logger.info(f"Uploaded batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1}")
        
        logger.info(f"Successfully uploaded {len(texts)} entities to Pinecone")

@flow(name="caaspp_last_3_years")
def caaspp_last_3_years():
    logger = get_run_logger()
    engine = create_engine(PG_URL, pool_pre_ping=True)

    now = datetime.datetime.utcnow()
    latest_year = now.year - 1  # AY 2024–25 → year_key 2024
    last_years = int(os.getenv("LAST_YEARS","3"))
    ensure_years(engine, latest_year, last_years)

    # Fetch StudentGroups and Tests reference files
    logger.info("Fetching StudentGroups reference file...")
    student_groups_bytes = http_get(STUDENT_GROUPS_URL)
    student_groups_df = None
    try:
        with zipfile.ZipFile(io.BytesIO(student_groups_bytes)) as z:
            for name in z.namelist():
                if name.lower().endswith('.txt') or name.lower().endswith('.csv'):
                    raw = z.read(name)
                    student_groups_df = parse_student_groups(raw)
                    logger.info(f"Loaded StudentGroups: {student_groups_df.height} rows")
                    break
    except Exception as e:
        logger.warning(f"Could not parse StudentGroups file: {e}")
    
    logger.info("Fetching Tests reference file...")
    tests_bytes = http_get(TESTS_URL)
    tests_df = None
    try:
        with zipfile.ZipFile(io.BytesIO(tests_bytes)) as z:
            for name in z.namelist():
                if name.lower().endswith('.txt') or name.lower().endswith('.csv'):
                    raw = z.read(name)
                    tests_df = parse_tests(raw)
                    logger.info(f"Loaded Tests: {tests_df.height} rows")
                    break
    except Exception as e:
        logger.warning(f"Could not parse Tests file: {e}")

    zip_blobs_for_entities = []

    for y in range(latest_year - last_years + 1, latest_year + 1):
        page = http_get.submit(CAASPP_LIST.format(year=y))
        zips = caret_zip_urls.submit(page)
        for u in zips.result():
            zb = http_get.submit(u)
            parts = parse_zip_caret(zb.result())
            # load tests/results
            load_tests.submit(engine, parts["tests"], y)
            zip_blobs_for_entities.append((y, zb.result()))

    # Build Pinecone index with all entity data
    build_pinecone_index.submit(zip_blobs_for_entities, student_groups_df, tests_df)

if __name__ == "__main__":
    caaspp_last_3_years()
