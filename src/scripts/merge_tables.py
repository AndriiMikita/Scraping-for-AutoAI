import os, json, pandas as pd, numpy as np
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, ElementTree
import ast

def ensure_dirs():
    os.makedirs("outputs",exist_ok=True)

def to_list(v):
    if v is None or (isinstance(v,float) and np.isnan(v)): return []
    if isinstance(v,list): return [str(x).strip() for x in v if str(x).strip()]
    try:
        x=ast.literal_eval(str(v))
        if isinstance(x,list): return [str(i).strip() for i in x if str(i).strip()]
    except: pass
    s=str(v).strip()
    return [s] if s else []

def clip_iqr_stats(s):
    x=pd.to_numeric(s,errors="coerce")
    xx=x.dropna()
    if xx.empty:
        return x,{"q1":None,"q3":None,"iqr":None,"low":None,"high":None,"n_low":0,"n_high":0,"pct":0.0}
    q1,q3=xx.quantile(0.25),xx.quantile(0.75)
    iqr=q3-q1
    low,high=q1-1.5*iqr,q3+1.5*iqr
    n_low=int((xx<low).sum())
    n_high=int((xx>high).sum())
    y=x.clip(lower=low,upper=high)
    pct=float((n_low+n_high)/max(1,len(xx)))
    return y,{"q1":float(q1), "q3":float(q3), "iqr":float(iqr), "low":float(low), "high":float(high), "n_low":n_low, "n_high":n_high, "pct":pct}

def seg(v):
    if pd.isna(v): return "unknown"
    r=float(v)
    if r<25: return "low-priced"
    if r<100: return "middle-priced"
    if r<200: return "high-priced"
    return "luxury"

def to_xml(df,path):
    r=Element("companies")
    for _,row in df.iterrows():
        c=SubElement(r,"company")
        SubElement(c,"company_name").text=str(row.get("company_name",""))
        SubElement(c,"source").text=str(row.get("source",""))
        SubElement(c,"rating").text=str(row.get("rating",""))
        SubElement(c,"reviews_count").text=str(row.get("reviews_count",""))
        SubElement(c,"hourly_mid").text=str(row.get("hourly_mid",""))
        SubElement(c,"min_project_usd").text=str(row.get("min_project_usd",""))
        SubElement(c,"team_mid").text=str(row.get("team_mid",""))
        SubElement(c,"price_segment").text=str(row.get("price_segment",""))
        L=SubElement(c,"locations")
        locs=row.get("locations")
        locs=to_list(locs)
        for loc in locs:
            SubElement(L,"location").text=str(loc)
        SubElement(c,"source_url").text=str(row.get("source_url",""))
        SubElement(c,"last_crawled_at").text=str(row.get("last_crawled_at",""))
    ElementTree(r).write(path,encoding="utf-8",xml_declaration=True)

def main():
    ensure_dirs()
    df=pd.read_csv("outputs/clean_raw.csv")
    if df.empty:
        print("no data"); return

    run_ts=datetime.now(datetime.timezone.utc).isoformat()
    rows_in=int(len(df))
    src_counts=df["source"].value_counts(dropna=False).to_dict()
    dup_map=df["company_name"].value_counts()
    dup_list={k:int(v) for k,v in dup_map[dup_map>1].sort_values(ascending=False).head(50).items()}

    df=df.sort_values(["company_name","reviews_count","rating"],ascending=[True,False,False])
    before=len(df)
    df=df.drop_duplicates(subset=["company_name"],keep="first")
    duplicates_removed=int(before-len(df))

    df["rating"]=pd.to_numeric(df["rating"],errors="coerce")
    df["hourly_mid"]=pd.to_numeric(df["hourly_mid"],errors="coerce")
    df["min_project_usd"]=pd.to_numeric(df["min_project_usd"],errors="coerce")
    df["team_mid"]=pd.to_numeric(df["team_mid"],errors="coerce")
    na_before=df[["rating","hourly_mid","min_project_usd","team_mid"]].isna().sum().to_dict()

    r_med=float(df["rating"].median(skipna=True)) if df["rating"].notna().any() else None
    if r_med is not None:
        df["rating"]=df["rating"].fillna(r_med)
    na_after_rating=int(df["rating"].isna().sum())

    medians={}
    fills={}
    for col in ["hourly_mid","min_project_usd","team_mid"]:
        gmed=df.groupby("source", dropna=True)[col].median()
        gmed_dict={str(k):float(v) for k,v in gmed.dropna().items()}
        glob=float(df[col].median(skipna=True)) if df[col].notna().any() else None
        def _fill_grp(s):
            m=gmed.get(s.name)
            if pd.isna(m):
                return s.fillna(glob)
            return s.fillna(m)
        df[col]=df.groupby("source")[col].transform(_fill_grp)
        medians[col]={"group_medians":gmed_dict,"global_median":glob}
        fills[col]={"filled":int(max(0,na_before.get(col,0)-df[col].isna().sum())),"na_before":int(na_before.get(col,0)),"na_after":int(df[col].isna().sum())}

    fills["rating"]={"filled":int(max(0,na_before.get("rating",0)-na_after_rating)),"na_before":int(na_before.get("rating",0)),"na_after":na_after_rating,"global_median":r_med}

    clip_stats={}
    for col in ["hourly_mid","min_project_usd","team_mid"]:
        df[col],st=clip_iqr_stats(df[col])
        clip_stats[col]=st

    df["price_segment"]=df["hourly_mid"].apply(seg)

    df.to_csv("outputs/merged.csv",index=False)
    to_xml(df,"outputs/merged.xml")

    ch={}
    ch["run_timestamp"]=run_ts
    ch["rows_in"]=rows_in
    ch["rows_after_dedup"]=int(len(df))
    ch["duplicates_removed"]=duplicates_removed
    ch["sources"]=src_counts
    ch["top_duplicate_names"]=dup_list
    ch["imputations"]=fills
    ch["medians_used"]=medians
    ch["iqr_clipping"]=clip_stats
    ch["segments"]=df["price_segment"].value_counts().to_dict()
    ch["numeric_summary"]=df[["rating","hourly_mid","min_project_usd","team_mid","reviews_count"]].describe(include="all").to_dict()

    with open("outputs/CHANGELOG.json","w",encoding="utf-8") as f: json.dump(ch,f,ensure_ascii=False,indent=2)

    with open("outputs/CHANGELOG.md","w",encoding="utf-8") as f:
        f.write("# Data Changes\n\n")
        f.write(f"- run_timestamp: {ch['run_timestamp']}\n")
        f.write(f"- rows_in: {ch['rows_in']}\n")
        f.write(f"- rows_after_dedup: {ch['rows_after_dedup']}\n")
        f.write(f"- duplicates_removed: {ch['duplicates_removed']}\n")
        f.write(f"- sources: {json.dumps(ch['sources'])}\n")
        f.write(f"- imputations: {json.dumps(ch['imputations'])}\n")
        f.write(f"- iqr_clipping: {json.dumps(ch['iqr_clipping'])}\n")
        f.write(f"- segments: {json.dumps(ch['segments'])}\n")

    print("ok")

if __name__=="__main__":
    main()
