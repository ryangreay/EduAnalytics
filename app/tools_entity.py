import os, faiss, numpy as np, polars as pl
from langchain_openai import OpenAIEmbeddings

class EntityResolver:
    def __init__(self, index_dir="./data/faiss"):
        idx = os.path.join(index_dir,"entities.faiss")
        meta = os.path.join(index_dir,"entities.parquet")
        self.enabled = os.path.exists(idx) and os.path.exists(meta)
        if self.enabled:
            self.index = faiss.read_index(idx)
            self.meta = pl.read_parquet(meta).to_pandas()
            self.emb = OpenAIEmbeddings(model="text-embedding-3-large")

    def search(self, text: str, k=5):
        if not self.enabled: return []
        v = np.array(self.emb.embed_query(text), dtype="float32")[None,:]
        faiss.normalize_L2(v)
        D,I = self.index.search(v,k)
        return self.meta.iloc[I[0]].to_dict(orient="records")
