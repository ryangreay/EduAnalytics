def pick_chart(df, question: str):
    q = question.lower()
    if df is None or len(df)==0: return {"type":"table"}
    if "breakdown" in q or "subgroup" in q or "ethnicity" in q:
        y = "prof_pct" if "prof_pct" in df[0] else next(iter(df[0].keys()))
        x = "subgroup" if "subgroup" in df[0] else next(iter(df[0].keys()))
        return {"type":"bar","x":x,"y":y}
    if "district" in q:
        y = "prof_pct" if "prof_pct" in df[0] else next(iter(df[0].keys()))
        x = "district" if "district" in df[0] else "district_name"
        return {"type":"bar","x":x,"y":y}
    if len(df)==1:
        # single metric → donut
        key = next((k for k in df[0].keys() if k.startswith("pct") or "prof" in k), None)
        if key: return {"type":"donut","x":key,"y":key}
    return {"type":"table"}
