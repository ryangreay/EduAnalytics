import io, re, zipfile
import polars as pl

def _norm(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({c: re.sub(r"[^a-z0-9]+","_",c.lower()).strip("_") for c in df.columns})

def read_caret_csv(raw: bytes) -> pl.DataFrame:
    return pl.read_csv(io.BytesIO(raw), separator="^", infer_schema_length=50000, null_values=["","NA","N/A"])

def parse_zip_caret(zip_bytes: bytes):
    out = {"entities": None, "tests": None}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            if not name.lower().endswith(".csv"): 
                continue
            raw = z.read(name)
            df = _norm(read_caret_csv(raw))
            cols = set(df.columns)
            # Entities file
            if {"county_code","district_code","school_code","type_id","test_year"}.issubset(cols):
                out["entities"] = df
            # Tests/results file
            elif {"test_type","test_id","student_group_id","total_students_tested_with_scores","mean_scale_score"}.issubset(cols):
                out["tests"] = df
    return out

def parse_student_groups(raw: bytes) -> pl.DataFrame:
    """Parse the StudentGroups.txt file"""
    df = read_caret_csv(raw)
    return _norm(df)

def parse_tests(raw: bytes) -> pl.DataFrame:
    """Parse the Tests.txt file"""
    df = read_caret_csv(raw)
    return _norm(df)