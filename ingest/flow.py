import os, re, datetime, json
import requests, polars as pl, numpy as np
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import faiss
from langchain_openai import OpenAIEmbeddings
from .config import DATA_DIR, CAASPP_LIST
from .transforms import parse_zip_caret

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
def build_faiss_from_entities(pairs: list[tuple[int, bytes]], index_dir: str = "./data/faiss"):
    os.makedirs(index_dir, exist_ok=True)
    emb = OpenAIEmbeddings(model="text-embedding-3-large")
    texts, metas = [], []
    for year_key, zip_bytes in pairs:
        parts = parse_zip_caret(zip_bytes)
        ents = parts["entities"]
        if ents is None or ents.height == 0:
            continue
        keep = [c for c in ["county_code","district_code","school_code","type_id","test_year","county_name","district_name","school_name","zip_code"] if c in ents.columns]
        ents = ents.select(keep).with_columns(pl.lit(year_key).alias("year_key"))
        for r in ents.iter_rows(named=True):
            label = " | ".join(
                [str(r.get("county_name") or ""), str(r.get("district_name") or ""), str(r.get("school_name") or "")]
            ).strip(" |")
            code = f"{r.get('county_code',''):0>2}{r.get('district_code',''):0>5}{r.get('school_code',''):0>7}"
            text = f"{label} | CDS:{code} | type:{r.get('type_id','')}"
            texts.append(text)
            metas.append(r)
    if not texts:
        return
    vecs = emb.embed_documents(texts)
    dim = len(vecs[0])
    idx = faiss.IndexFlatIP(dim)
    mat = np.array(vecs, dtype="float32")
    faiss.normalize_L2(mat)
    idx.add(mat)
    faiss.write_index(idx, os.path.join(index_dir,"entities.faiss"))
    pl.DataFrame(metas).write_parquet(os.path.join(index_dir,"entities.parquet"))
    with open(os.path.join(index_dir,"meta.json"),"w") as f:
        json.dump({"count": len(texts)}, f)

@flow(name="caaspp_last_3_years")
def caaspp_last_3_years():
    logger = get_run_logger()
    engine = create_engine(PG_URL, pool_pre_ping=True)

    now = datetime.datetime.utcnow()
    latest_year = now.year - 1  # AY 2024–25 → year_key 2024
    last_years = int(os.getenv("LAST_YEARS","3"))
    ensure_years(engine, latest_year, last_years)

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

    build_faiss_from_entities.submit(zip_blobs_for_entities)

if __name__ == "__main__":
    caaspp_last_3_years()
