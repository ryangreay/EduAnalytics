import os, re, datetime, json, zipfile, io
from typing import Optional
import requests, polars as pl, numpy as np
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pinecone import Pinecone, ServerlessSpec
from pinecone.core.openapi.shared.exceptions import NotFoundException
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from .config import DATA_DIR, CAASPP_LIST, STUDENT_GROUPS_URL, TESTS_URL
from .transforms import parse_zip_caret, parse_student_groups, parse_tests
import time

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
    logger = get_run_logger()
    hrefs = re.findall(rb'href="([^"]+\.zip)"', list_html, re.IGNORECASE)
    logger.info(f"Found {len(hrefs)} total .zip hrefs in HTML")
    
    out = []
    for h in hrefs:
        u = h.decode("utf-8")
        if u.startswith("/"):
            u = "https://caaspp-elpac.ets.org" + u
        
        # Look for Smarter Balanced files with CSV format and "all" (combined data)
        # Exclude subject-specific files (math/ela only)
        if "sb_" in u.lower() and "csv" in u.lower() and "all" in u.lower() and "math" not in u.lower() and "ela" not in u.lower():
            out.append(u)
    
    logger.info(f"Returning {len(out)} matching URLs")
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
def delete_scores_for_year(engine, year_key: int):
    logger = get_run_logger()
    with engine.begin() as con:
        logger.info(f"Deleting existing analytics.fact_scores rows for year_key={year_key}...")
        res = con.execute(text("""
            DELETE FROM analytics.fact_scores WHERE year_key = :y
        """), {"y": year_key})
        try:
            deleted = res.rowcount if hasattr(res, "rowcount") else None
            if deleted is not None:
                logger.info(f"Deleted {deleted:,} rows for year_key={year_key}")
        except Exception:
            pass

@task
def load_tests(engine, tests: pl.DataFrame, year_key: int, county_lookup: Optional[pl.DataFrame] = None):
    logger = get_run_logger()
    if tests is None or tests.height == 0:
        return
    
    # Normalize column names - handle both old and new formats
    # First, standardize column names if needed
    df = tests
    
    # Map new column names to old expected names
    if "students_tested" in df.columns and "total_students_tested" not in df.columns:
        df = df.rename({"students_tested": "total_students_tested"})
    
    if "total_tested_with_scores_at_reporting_level" in df.columns and "total_students_tested_with_scores" not in df.columns:
        df = df.rename({"total_tested_with_scores_at_reporting_level": "total_students_tested_with_scores"})
    elif "students_with_scores" in df.columns and "total_students_tested_with_scores" not in df.columns:
        df = df.rename({"students_with_scores": "total_students_tested_with_scores"})
    
    # Now map to final column names for database
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
        "county_code":"county_code",
        "district_code":"district_code",
        "school_code":"school_code",
        "district_name":"district_name",
        "school_name":"school_name",
        "test_id":"test_id"
    }
    
    cols = [c for c in ren if c in df.columns]
    rename_map = {c: ren[c] for c in cols}
    df = df.select(cols).rename(rename_map)
    
    if "test_id" not in df.columns:
        logger.warning("test_id column not found, cannot determine subject. Skipping this data.")
        return
    
    # Add year_key immediately (no conflict)
    df = df.with_columns(pl.lit(year_key).alias("year_key"))

    # Safely pad codes by writing to temp columns, then replace originals to avoid duplicate alias errors
    if "county_code" in df.columns:
        df = df.with_columns(pl.col("county_code").cast(pl.Utf8).str.zfill(2).alias("county_code_p"))
        df = df.drop(["county_code"]).rename({"county_code_p": "county_code"})
    if "district_code" in df.columns:
        df = df.with_columns(pl.col("district_code").cast(pl.Utf8).str.zfill(5).alias("district_code_p"))
        df = df.drop(["district_code"]).rename({"district_code_p": "district_code"})
    if "school_code" in df.columns:
        df = df.with_columns(pl.col("school_code").cast(pl.Utf8).str.zfill(7).alias("school_code_p"))
        df = df.drop(["school_code"]).rename({"school_code_p": "school_code"})

    # Populate cds_code from padded codes
    if "county_code" in df.columns and "district_code" in df.columns and "school_code" in df.columns:
        df = df.with_columns(
            (
                pl.col("county_code").cast(pl.Utf8).str.zfill(2)
                + pl.col("district_code").cast(pl.Utf8).str.zfill(5)
                + pl.col("school_code").cast(pl.Utf8).str.zfill(7)
            ).alias("cds_code")
        )

    # Join county_name from entities-derived lookup if provided
    if county_lookup is not None and "county_code" in df.columns:
        try:
            cl = county_lookup
            # Ensure normalized/padded codes in the lookup as well
            if "county_code" in cl.columns:
                cl = cl.with_columns(
                    pl.col("county_code").cast(pl.Utf8).str.zfill(2).alias("county_code")
                )
            # Only keep necessary columns and unique codes
            keep_cols = [c for c in ["county_code", "county_name"] if c in cl.columns]
            if "county_name" in keep_cols:
                cl = cl.select(keep_cols).unique(subset=["county_code"], keep="first")
                df = df.join(cl, on="county_code", how="left")
        except Exception:
            # Best-effort enrichment; continue without blocking load
            pass

    # Ensure COPY column order and presence to avoid positional mismatches
    expected_cols = [
        "subgroup", "grade", "tested", "tested_with_scores", "mean_scale_score",
        "pct_exceeded", "cnt_exceeded", "pct_met", "cnt_met",
        "pct_met_and_above", "cnt_met_and_above", "pct_nearly_met", "cnt_nearly_met",
        "pct_not_met", "cnt_not_met",
        "county_name",
        "county_code", "district_code", "school_code",
        "district_name", "school_name", "test_id", "year_key"
    ]

    # Add any missing columns as nulls so counts don't receive percentages by shift
    for col in expected_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    # Reorder strictly to COPY list
    df = df.select(expected_cols)
    
    with engine.begin() as con:
        raw_conn = con.connection.driver_connection
        
        buffer = io.StringIO()
        pandas_df = df.to_pandas()
        pandas_df.to_csv(buffer, index=False, header=False, na_rep='\\N')
        buffer.seek(0)
        
        copy_sql = """
            COPY analytics.fact_scores(
                subgroup, grade, tested, tested_with_scores, mean_scale_score,
                pct_exceeded, cnt_exceeded, pct_met, cnt_met,
                pct_met_and_above, cnt_met_and_above, pct_nearly_met, cnt_nearly_met,
                pct_not_met, cnt_not_met, 
                county_name,
                county_code, district_code, school_code,
                district_name, school_name, test_id, year_key
            )
            FROM STDIN WITH (FORMAT CSV, NULL '\\N')
        """
        
        logger.info(f"Starting COPY command for bulk insert...")
        with raw_conn.cursor() as cursor:
            with cursor.copy(copy_sql) as copy:
                while True:
                    data = buffer.read(8192)
                    if not data:
                        break
                    copy.write(data)
        logger.info(f"Successfully inserted {df.shape[0]:,} rows")

@task
def build_pinecone_index(entity_dataframes: list[tuple[int, pl.DataFrame]], student_groups_df: pl.DataFrame, tests_df: pl.DataFrame):
    """Build Pinecone index with entities (from most recent year only), student groups, tests, grades"""
    logger = get_run_logger()
    
    # Check if we have any data to index
    logger.info(f"Number of entity dataframes: {len(entity_dataframes)}")
    if len(entity_dataframes) == 0:
        logger.warning("No entity dataframes provided - will only index student groups, tests, and grades")
    
    for y, ents in entity_dataframes:
        logger.info(f"Entity dataframe for year {y}: {ents.height} rows")

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
    index_already_existed = index_name in existing_indexes
    
    logger.info(f"Existing indexes: {existing_indexes}")
    logger.info(f"Index {index_name} already exists: {index_already_existed}")

    if not index_already_existed:
        logger.info(f"Creating Pinecone index: {index_name}")
        pc.create_index(
            name=index_name,
            dimension=3072,  # text-embedding-3-large dimension
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1")
        )
        # Wait for index to be ready
        logger.info(f"Waiting for index {index_name} to be ready...")
        
        while not pc.describe_index(index_name).status.ready:
            time.sleep(1)
        logger.info(f"Index {index_name} is ready!")
    
    # Initialize embeddings and vector store
    embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
    vector_store = PineconeVectorStore(index_name=index_name, embedding=embeddings)
    
    texts, metadatas = [], []
    
    # 1. Add entities (counties, districts, schools)
    for year_key, ents in entity_dataframes:
        if ents is None or ents.height == 0:
            continue
        
        for r in ents.iter_rows(named=True):
            county_name = str(r.get("county_name") or "").strip() + " County"
            district_name = str(r.get("district_name") or "").strip()
            school_name = str(r.get("school_name") or "").strip()
            county_code = str(r.get("county_code") or "").strip().zfill(2)
            district_code = str(r.get("district_code") or "").strip().zfill(5)
            school_code = str(r.get("school_code") or "").strip().zfill(7)
            
            label = " | ".join([county_name, district_name, school_name]).strip(" |")
            text = f"{label} | County:{county_code} District:{district_code} School:{school_code}"
            texts.append(text)
            metadatas.append({
                "type": "entity",
                "county_name": county_name,
                "district_name": district_name,
                "school_name": school_name,
                "county_code": county_code,
                "district_code": district_code,
                "school_code": school_code,
                "year_key": year_key
            })
    
    logger.info(f"Number of entities added: {len(texts)}")
    
    # 2. Add student groups/subgroups
    if student_groups_df is not None and student_groups_df.height > 0:
        for r in student_groups_df.iter_rows(named=True):
            demo_name = str(r.get("demographic_name", ""))
            demo_id = int(r.get("demographic_id_num", ""))
            student_group = str(r.get("student_group", ""))
            
            text = f"{demo_name} (Subgroup ID: {demo_id}, Category: {student_group})"
            texts.append(text)
            metadatas.append({
                "type": "subgroup",
                "demographic_name": demo_name,
                "demographic_id": demo_id,
                "student_group": student_group
            })

    logger.info(f"Number of student groups added: {len(texts)}")

    # 3. Add tests
    if tests_df is not None and tests_df.height > 0:
        for r in tests_df.iter_rows(named=True):
            test_name = str(r.get("test_name", ""))
            test_id = int(r.get("test_id_num", ""))
            
            text = f"{test_name} (Test ID: {test_id})"
            texts.append(text)
            metadatas.append({
                "type": "test",
                "test_name": test_name,
                "test_id": test_id
            })

    logger.info(f"Number of tests added: {len(texts)}")

    # 4. Add grades
    grades = [3, 4, 5, 6, 7, 8, 11, 13]
    for grade in grades:
        grade = int(grade)
        texts.append(f"Grade {grade}" if grade != 13 else "All Grades")
        metadatas.append({"type": "grade", "grade": grade})

    logger.info(f"Grade texts added: {texts}, metadatas: {metadatas}")

    # Upload to Pinecone in batches
    if texts:
        logger.info(f"Uploading {len(texts)} entities to Pinecone index: {index_name}")
        
        # Clear existing data only if index already existed
        if index_already_existed:
            logger.info(f"Clearing existing data from Pinecone index: {index_name}")
            try:
                pc.Index(index_name).delete(delete_all=True)
            except NotFoundException:
                # If namespace doesn't exist (empty index), that's fine - nothing to clear
                logger.info(f"No existing data to clear (namespace not found) - index is empty")
        
        # Add in batches
        logger.info(f"Adding data to Pinecone index: {index_name}")
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
    latest_year = now.year #- 1  # AY 2024–25 → year_key 2024
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

    entity_dataframes = []
    load_tasks = []

    for y in range(latest_year - last_years + 1, latest_year + 1):
        logger.info(f"Processing year {y} (latest_year={latest_year})")
        # Ensure we start fresh for this year: remove any previously loaded rows
        delete_scores_for_year.submit(engine, y)
        page = http_get.submit(CAASPP_LIST.format(year=y))
        zips = caret_zip_urls.submit(page)
        zip_urls = zips.result()
        logger.info(f"Found {len(zip_urls)} zip files for year {y}")
        
        for u in zip_urls:
            logger.info(f"Downloading {u}")
            zb = http_get.submit(u)
            parts = parse_zip_caret(zb.result(), logger=logger)
            
            # log what we got
            logger.info(f"Entities: {parts['entities'].height if parts['entities'] is not None else 'None'} rows")
            logger.info(f"Tests: {parts['tests'].height if parts['tests'] is not None else 'None'} rows")
            
            # load tests/results (enrich with county_name via entities lookup when available)
            county_lookup = None
            ents_for_lookup = parts.get("entities")
            if ents_for_lookup is not None and {"county_code","county_name"}.issubset(set(ents_for_lookup.columns)):
                county_lookup = ents_for_lookup.select(["county_code","county_name"]).unique(subset=["county_code"], keep="first")

            task = load_tests.submit(engine, parts["tests"], y, county_lookup)
            load_tasks.append(task)
            
            # Parse and prepare entities for Pinecone (only for latest year)
            if y == latest_year:
                logger.info(f"Year {y} matches latest_year {latest_year}, collecting entities...")
                ents = parts["entities"]
                if ents is not None and ents.height > 0:
                    keep = [c for c in ["county_code","district_code","school_code","type_id","test_year","county_name","district_name","school_name","zip_code"] if c in ents.columns]
                    ents = ents.select(keep).with_columns(pl.lit(y).alias("year_key"))
                    entity_dataframes.append((y, ents))
                    logger.info(f"Collected {ents.height} entities for year {y}")
                else:
                    logger.warning(f"No entities found for year {y}")

    # Wait for all load_tests tasks to complete
    logger.info("Waiting for all load_tests tasks to complete...")
    for task in load_tasks:
        task.wait()
    logger.info("All load_tests tasks completed")

    # Build Pinecone index with entity data from latest year only
    build_pinecone_index.submit(entity_dataframes, student_groups_df, tests_df)

if __name__ == "__main__":
    caaspp_last_3_years()
