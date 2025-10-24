import io, re, zipfile
import polars as pl
import logging

def _norm(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({c: re.sub(r"[^a-z0-9]+","_",c.lower()).strip("_") for c in df.columns})

def read_caret_csv(raw: bytes) -> pl.DataFrame:
    return pl.read_csv(io.BytesIO(raw), separator="^", infer_schema_length=50000, null_values=["","NA","N/A", "*"], encoding="utf8-lossy")

def read_comma_csv(raw: bytes) -> pl.DataFrame:
    return pl.read_csv(io.BytesIO(raw), separator=",", infer_schema_length=50000, null_values=["","NA","N/A", "*"], encoding="utf8-lossy")

def parse_zip_caret(zip_bytes: bytes, logger=None):
    out = {"entities": None, "tests": None}
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            filenames = z.namelist()
            logger.info(f"Zip contains {len(filenames)} files: {filenames}")
            
            for name in filenames:
                logger.info(f"Processing file: {name}")
                # Accept both .csv and .txt files (sometimes they're mislabeled)
                if not (name.lower().endswith(".csv") or name.lower().endswith(".txt")): 
                    continue
                
                logger.info(f"Reading {name} ({z.getinfo(name).file_size} bytes)...")
                raw = z.read(name)
                
                df = _norm(read_caret_csv(raw))
                
                cols = set(df.columns)
                if {"test_type","test_id","student_group_id","mean_scale_score"}.issubset(cols) and \
                   ("total_students_tested_with_scores" in cols or "total_tested_with_scores_at_reporting_level" in cols):
                    out["tests"] = df
                elif {"county_code","district_code","school_code","type_id","test_year"}.issubset(cols) and \
                     "test_type" not in cols:
                    out["entities"] = df
                else:
                    logger.warning(f"File {name} doesn't match entities or tests schema. Columns: {cols}")
    except Exception as e:
        logger.error(f"Error parsing zip file: {e}", exc_info=True)
    
    return out

def parse_student_groups(raw: bytes) -> pl.DataFrame:
    """Parse the StudentGroups.txt file (comma-delimited CSV)"""
    df = read_comma_csv(raw)
    return _norm(df)

def parse_tests(raw: bytes) -> pl.DataFrame:
    """Parse the Tests.txt file (caret-delimited)"""
    df = read_caret_csv(raw)
    return _norm(df)