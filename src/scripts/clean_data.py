import os, re, json, psycopg2, pandas as pd, numpy as np
from urllib.parse import urlparse
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, ElementTree

def dbp():
    return dict(user=os.getenv("POSTGRES_USER","market"),
                password=os.getenv("POSTGRES_PASSWORD","marketpass"),
                host=os.getenv("DB_HOST","db"),
                port=os.getenv("DB_PORT","5432"),
                dbname=os.getenv("POSTGRES_DB","marketdb"))

def fetch():
    q="""SELECT id,source_url,company_name,rating,reviews_count,hourly_rate,
                 min_project_size,team_size,last_crawled_at,locations,services_offered
         FROM market_entries
         WHERE company_name IS NOT NULL"""
    with psycopg2.connect(**dbp()) as c:
        with c.cursor() as cur:
            cur.execute(q)
            cols=[d[0] for d in cur.description]
            rows=cur.fetchall()
    return pd.DataFrame(rows,columns=cols)

def dmn(u):
    try: return urlparse(u).netloc.lower()
    except: return None

def mrange(s):
    if not s: return (None,None)
    t=str(s).strip()
    m=re.search(r"\$?\s?(\d[\d,]*)\s*-\s*\$?\s?(\d[\d,]*)",t)
    if m: return (int(m.group(1).replace(",","")),int(m.group(2).replace(",","")))
    m=re.search(r"<\s*\$?\s?(\d[\d,]*)",t)
    if m: return (0,int(m.group(1).replace(",","")))
    m=re.search(r"\$?\s?(\d[\d,]*)\s*\+",t)
    if m:
        v=int(m.group(1).replace(",",""))
        return (v,v)
    m=re.search(r"\$?\s?(\d[\d,]*)",t)
    if m:
        v=int(m.group(1).replace(",",""))
        return (v,v)
    return (None,None)

def trange(s):
    if not s: return (None,None)
    t=str(s).replace(","," ")
    m=re.search(r"(\d+)\s*-\s*(\d+)",t)
    if m: return (int(m.group(1)),int(m.group(2)))
    m=re.search(r"(\d+)",t)
    if m:
        v=int(m.group(1))
        return (v,v)
    return (None,None)

def mid(a,b):
    if a is None and b is None: return None
    if a is None: return b
    if b is None: return a
    return (a+b)/2

def norm_list(v):
    if v is None: return []
    if isinstance(v,list): return [str(x).strip() for x in v if str(x).strip()]
    try:
        x=json.loads(v)
        if isinstance(x,list): return [str(i).strip() for i in x if str(i).strip()]
    except: pass
    s=str(v).strip()
    return [s] if s else []

rx_ai = re.compile(r"\b(ai|artificial intelligence|machine learning|ml|computer vision|nlp|natural language processing|deep learning)\b", re.I)
rx_iot = re.compile(r"\b(iot|internet of things)\b", re.I)
rx_mobile = re.compile(r"\b(mobile|android|ios|iphone|ipad|flutter|react native|mobile app)\b", re.I)

def has_ai(lst): return any(rx_ai.search(str(x)) for x in lst)
def has_iot(lst): return any(rx_iot.search(str(x)) for x in lst)
def has_mobile(lst): return any(rx_mobile.search(str(x)) for x in lst)

def ensure_dirs():
    os.makedirs("outputs",exist_ok=True)

def to_xml(df,path):
    r=Element("companies")
    for _,row in df.iterrows():
        c=SubElement(r,"company")
        SubElement(c,"company_name").text=str(row.get("company_name",""))
        SubElement(c,"source").text=str(row.get("source",""))
        SubElement(c,"rating").text=str(row.get("rating",""))
        SubElement(c,"reviews_count").text=str(row.get("reviews_count",""))
        SubElement(c,"hourly_rate").text=str(row.get("hourly_rate",""))
        SubElement(c,"min_project_size").text=str(row.get("min_project_size",""))
        SubElement(c,"team_size").text=str(row.get("team_size",""))
        L=SubElement(c,"locations")
        for loc in row.get("locations") or []:
            SubElement(L,"location").text=str(loc)
        S=SubElement(c,"services")
        for s in row.get("services_offered") or []:
            SubElement(S,"service").text=str(s)
        SubElement(c,"svc_ai").text=str(int(row.get("svc_ai",0)))
        SubElement(c,"svc_iot").text=str(int(row.get("svc_iot",0)))
        SubElement(c,"svc_mobile").text=str(int(row.get("svc_mobile",0)))
        SubElement(c,"source_url").text=str(row.get("source_url",""))
        SubElement(c,"last_crawled_at").text=str(row.get("last_crawled_at",""))
    ElementTree(r).write(path,encoding="utf-8",xml_declaration=True)

def iqr_outliers_count(x):
    v=pd.to_numeric(x,errors="coerce").dropna()
    if v.empty: return {"count":0,"lower":None,"upper":None}
    q1,q3=np.percentile(v,[25,75])
    iqr=q3-q1
    lo=q1-1.5*iqr
    hi=q3+1.5*iqr
    m=(v<lo)|(v>hi)
    return {"count":int(m.sum()),"lower":float(lo),"upper":float(hi)}

def numeric_stats(x):
    v=pd.to_numeric(x,errors="coerce")
    return {
        "count": int(v.notna().sum()),
        "mean": float(v.mean()) if v.notna().any() else None,
        "median": float(v.median()) if v.notna().any() else None,
        "min": float(v.min()) if v.notna().any() else None,
        "max": float(v.max()) if v.notna().any() else None,
        "std": float(v.std()) if v.notna().any() else None
    }

def main():
    ensure_dirs()
    df=fetch()
    if df.empty:
        print("no data"); return
    df["company_name"]=df["company_name"].astype(str).str.strip()
    df=df[df["company_name"].notna() & (df["company_name"]!="")]
    df["source"]=df["source_url"].apply(dmn)
    df["rating"]=pd.to_numeric(df["rating"],errors="coerce")
    df["reviews_count"]=pd.to_numeric(df["reviews_count"],errors="coerce").fillna(0).astype(int)
    df["locations"]=df["locations"].apply(norm_list)
    df["services_offered"]=df["services_offered"].apply(norm_list)
    rates=df["hourly_rate"].apply(mrange)
    df["hourly_low"]=rates.apply(lambda x:x[0])
    df["hourly_high"]=rates.apply(lambda x:x[1])
    df["hourly_mid"]=df.apply(lambda r:mid(r["hourly_low"],r["hourly_high"]),axis=1)
    mps=df["min_project_size"].apply(mrange)
    df["min_project_usd"]=mps.apply(lambda x:x[0])
    trs=df["team_size"].apply(trange)
    df["team_low"]=trs.apply(lambda x:x[0])
    df["team_high"]=trs.apply(lambda x:x[1])
    df["team_mid"]=df.apply(lambda r:mid(r["team_low"],r["team_high"]),axis=1)
    df["svc_ai"]=df["services_offered"].apply(has_ai).astype(int)
    df["svc_iot"]=df["services_offered"].apply(has_iot).astype(int)
    df["svc_mobile"]=df["services_offered"].apply(has_mobile).astype(int)
    df.to_csv("outputs/clean_raw.csv",index=False)
    to_xml(df,"outputs/clean_raw.xml")

    cols_base=["company_name","source","rating","reviews_count","hourly_rate","min_project_size","team_size","locations","services_offered"]
    null_counts={c:int(df[c].isna().sum()) for c in cols_base if c in df.columns}
    completeness={c:float((df[c].notna() & (df[c].astype(str)!="")).mean()) for c in cols_base if c in df.columns}
    by_source=df["source"].value_counts().to_dict()
    uniq_companies=int(df["company_name"].nunique())
    dups=int(len(df)-len(df.drop_duplicates(subset=["company_name","source"], keep="first")))
    num_fields=["rating","reviews_count","hourly_low","hourly_high","hourly_mid","min_project_usd","team_low","team_high","team_mid"]
    stats={k:numeric_stats(df[k]) for k in num_fields if k in df.columns}
    outliers={k:iqr_outliers_count(df[k]) for k in ["rating","reviews_count","hourly_mid","team_mid","min_project_usd"] if k in df.columns}
    meta={
        "timestamp": datetime.utcnow().isoformat(),
        "rows_total": int(len(df)),
        "unique_companies": uniq_companies,
        "possible_duplicates": dups,
        "by_source": by_source,
        "null_counts": null_counts,
        "completeness": completeness,
        "numeric_stats": stats,
        "outliers_1_5_IQR": outliers,
        "columns": list(df.columns)
    }
    with open("outputs/clean_raw_meta.json","w",encoding="utf-8") as f:
        json.dump(meta,f,ensure_ascii=False,indent=2)
    print("ok")

if __name__=="__main__":
    main()
