import os, re, ast
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def ensure_dirs():
    os.makedirs("outputs/plots", exist_ok=True)

def parse_list(s):
    if pd.isna(s) or s == "": return []
    if isinstance(s, list): return [str(x).strip() for x in s if str(x).strip()]
    try:
        v = ast.literal_eval(str(s))
        if isinstance(v, list): return [str(x).strip() for x in v if str(x).strip()]
    except: pass
    return [str(s).strip()] if str(s).strip() else []

def region_from_locations(s):
    lst = parse_list(s)
    if not lst:
        return "Unknown"
    first = str(lst[0])
    parts = [p.strip() for p in first.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[1]
    return parts[-1] if parts else "Unknown"

_money = re.compile(r"[\d,.]+")

def parse_money_hourly(s):
    s = "" if pd.isna(s) else str(s)
    nums = [float(x.replace(",", "")) for x in _money.findall(s)]
    tl = s.lower()
    if "hour" in tl and "-" in s and len(nums) >= 2:
        lo, hi = nums[0], nums[1]; return lo, hi, np.nanmean([lo, hi])
    if s.strip().startswith("<") and "hour" in tl and nums:
        return 0.0, nums[0], nums[0] / 2.0
    if "hour" in tl and nums:
        v = float(nums[0]); return v, v, v
    if nums and "+" in s:
        v = float(nums[0]); return v, np.nan, v
    if nums:
        v = float(nums[0]); return v, np.nan, v
    return np.nan, np.nan, np.nan

def parse_team(s):
    s = "" if pd.isna(s) else str(s)
    nums = [int(float(x.replace(",", ""))) for x in _money.findall(s)]
    if "-" in s and len(nums) >= 2:
        lo, hi = nums[0], nums[1]; return lo, hi, (lo + hi) / 2.0
    if nums:
        v = nums[0]; return v, v, float(v)
    return np.nan, np.nan, np.nan

def price_segment(mid):
    if pd.isna(mid): return "Unknown"
    x = float(mid)
    if x < 25: return "low-priced"
    if x < 50: return "middle-priced"
    if x < 100: return "high-priced"
    return "luxury"

def min_project_bucket(s):
    s = "" if pd.isna(s) else str(s)
    nums = [int(float(x.replace(",", ""))) for x in _money.findall(s)]
    v = nums[0] if nums else np.nan
    if pd.isna(v): return "Unknown"
    if v < 5000: return "< $5k"
    if v < 10000: return "$5k–$10k"
    if v < 25000: return "$10k–$25k"
    if v < 50000: return "$25k–$50k"
    if v < 100000: return "$50k–$100k"
    return "$100k+"

def employee_bucket(mid):
    if pd.isna(mid): return "Unknown"
    x = float(mid)
    if x < 10: return "<10"
    if x < 50: return "10–49"
    if x < 100: return "50–99"
    if x < 250: return "100–249"
    if x < 1000: return "250–999"
    return "1000+"

rx_ai = re.compile(r"\b(ai|artificial intelligence|machine learning|ml|computer vision|nlp|natural language processing|deep learning)\b", re.I)
rx_iot = re.compile(r"\b(iot|internet of things)\b", re.I)
rx_mobile = re.compile(r"\b(mobile|android|ios|iphone|ipad|flutter|react native|mobile app)\b", re.I)

def has_ai(lst): return any(rx_ai.search(str(x)) for x in lst)
def has_iot(lst): return any(rx_iot.search(str(x)) for x in lst)
def has_mobile(lst): return any(rx_mobile.search(str(x)) for x in lst)

def save_bar(series, title, xlabel, ylabel, path, top=None, sort_desc=True, rotate=False):
    s = series.dropna()
    if top is not None:
        s = s.sort_values(ascending=not sort_desc).head(int(top))
    if len(s) == 0:
        return
    plt.figure()
    ax = s.plot(kind="bar")
    if rotate and hasattr(ax, "set_xticklabels"):
        ax.set_xticklabels([str(x) for x in s.index], rotation=45, ha="right")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def save_hist(series, title, xlabel, path, bins=20):
    x = pd.to_numeric(series, errors="coerce").dropna()
    if len(x) == 0: return
    plt.figure()
    x.plot(kind="hist", bins=bins)
    plt.title(title); plt.xlabel(xlabel); plt.ylabel("Count")
    plt.tight_layout(); plt.savefig(path); plt.close()

def save_scatter(x, y, title, xlabel, ylabel, path, size=None):
    xv = pd.to_numeric(x, errors="coerce")
    yv = pd.to_numeric(y, errors="coerce")
    m = xv.notna() & yv.notna()
    if m.sum() == 0: return
    plt.figure()
    if size is not None:
        sv = pd.to_numeric(size, errors="coerce")
        s = sv[m].fillna(sv[m].median() if sv[m].notna().any() else 20)
        plt.scatter(xv[m], yv[m], s=(s / s.max()) * 80 + 10)
    else:
        plt.scatter(xv[m], yv[m], s=12)
    plt.title(title); plt.xlabel(xlabel); plt.ylabel(ylabel)
    plt.tight_layout(); plt.savefig(path); plt.close()

def main():
    ensure_dirs()
    df = pd.read_csv("outputs/merged.csv")
    if df.empty:
        print("no data"); return

    if "source" not in df.columns:
        df["source"] = df["source_url"].fillna("").apply(lambda u: re.sub(r"^https?://(www\.)?","",u).split("/")[0] if isinstance(u,str) else "")
    if "hourly_mid" not in df.columns:
        parsed = df["hourly_rate"].fillna("").apply(parse_money_hourly)
        df["hourly_min"] = parsed.apply(lambda t: t[0])
        df["hourly_max"] = parsed.apply(lambda t: t[1])
        df["hourly_mid"] = parsed.apply(lambda t: t[2])
    if "team_mid" not in df.columns:
        tparsed = df["team_size"].fillna("").apply(parse_team)
        df["team_min"] = tparsed.apply(lambda t: t[0])
        df["team_max"] = tparsed.apply(lambda t: t[1])
        df["team_mid"] = tparsed.apply(lambda t: t[2])
    if "price_segment" not in df.columns:
        df["price_segment"] = df["hourly_mid"].apply(price_segment)
    if "min_project_bucket" not in df.columns:
        df["min_project_bucket"] = df["min_project_size"].apply(min_project_bucket)
    df["region"] = df["locations"].apply(region_from_locations)

    svc = df.get("services_offered")
    svc_lists = svc.fillna("").apply(parse_list) if svc is not None else pd.Series([[]]*len(df))
    df["svc_ai"] = svc_lists.apply(has_ai).astype(int)
    df["svc_iot"] = svc_lists.apply(has_iot).astype(int)
    df["svc_mobile"] = svc_lists.apply(has_mobile).astype(int)

    save_bar(df["source"].value_counts(), "Records by Source", "Source", "Count", "outputs/plots/01_by_source.png")
    save_bar(df["price_segment"].value_counts(), "Records by Price Segment", "Segment", "Count", "outputs/plots/02_by_segment.png")
    save_hist(df["hourly_mid"], "Hourly Rate (mid) Distribution", "USD/hour", "outputs/plots/03_hourly_hist.png", bins=25)
    save_scatter(df["hourly_mid"], df["rating"], "Rating vs Hourly Rate", "Hourly mid (USD)", "Rating", "outputs/plots/04_rating_vs_hourly.png", size=df.get("reviews_count"))
    save_bar(df["region"].value_counts(), "Records by Region (2nd location part)", "Region", "Count", "outputs/plots/05_by_region.png", top=30, sort_desc=True, rotate=True)
    avg_rating_region = df.groupby("region")["rating"].mean().dropna().sort_values(ascending=False)
    save_bar(avg_rating_region.head(20), "Avg Rating by Region (top 20)", "Region", "Avg rating", "outputs/plots/06_avg_rating_by_region.png", sort_desc=True, rotate=True)
    med_rate_emp = df.groupby(df["team_mid"].apply(employee_bucket))["hourly_mid"].median().dropna().sort_index()
    save_bar(med_rate_emp, "Median Hourly by Employees Bucket", "Employees bucket", "USD/hour", "outputs/plots/07_median_hourly_by_employees.png")
    save_bar(df["min_project_bucket"].value_counts(), "Min Project Size Buckets", "Bucket", "Count", "outputs/plots/08_min_project_buckets.png")

    svc_counts = pd.Series({"AI": int(df["svc_ai"].sum()), "IoT": int(df["svc_iot"].sum()), "Mobile": int(df["svc_mobile"].sum())})
    save_bar(svc_counts, "Records by Service Type (AI/IoT/Mobile)", "Service", "Count", "outputs/plots/09_by_service_type.png")

    mean_rating_by_service = pd.Series({
        "AI": df.loc[df["svc_ai"]==1, "rating"].mean(),
        "IoT": df.loc[df["svc_iot"]==1, "rating"].mean(),
        "Mobile": df.loc[df["svc_mobile"]==1, "rating"].mean(),
    }).dropna()
    print("MEAN RATE/SERVICE: ", mean_rating_by_service)
    save_bar(mean_rating_by_service, "Mean Rating by Service Type", "Service", "Mean rating", "outputs/plots/10_mean_rating_by_service.png")

    med_hourly_by_service = pd.Series({
        "AI": df.loc[df["svc_ai"]==1, "hourly_mid"].median(),
        "IoT": df.loc[df["svc_iot"]==1, "hourly_mid"].median(),
        "Mobile": df.loc[df["svc_mobile"]==1, "hourly_mid"].median(),
    }).dropna()
    save_bar(med_hourly_by_service, "Median Hourly by Service Type", "Service", "USD/hour", "outputs/plots/11_median_hourly_by_service.png")

    df.groupby(["price_segment","region"]).size().reset_index(name="n").sort_values("n", ascending=False).head(200).to_csv("outputs/plots/_segment_region_top200.csv", index=False)
    df.assign(service_type=np.select(
        [df["svc_ai"].eq(1), df["svc_iot"].eq(1), df["svc_mobile"].eq(1)],
        ["AI","IoT","Mobile"], default="Other"
    )).groupby(["service_type","price_segment"]).size().reset_index(name="n").to_csv("outputs/plots/_service_price_matrix.csv", index=False)
    print("ok")

if __name__ == "__main__":
    main()
