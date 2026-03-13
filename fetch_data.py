"""
Fixed Income Terminal — Data Fetcher v3
=========================================
Sources (all free):
  US Treasury XML         → US yield curve        (no key)
  FRED API                → spreads, SOFR, history (free key: fred.stlouisfed.org/docs/api/api_key.html)
  ECB SDW REST API        → EU/Bund curves         (no key)
  Bank of Japan CSV       → JGB yields             (no key)
  FINRA Fixed Income API  → Agency (FNMA/FHLB/FHLMC) + Corp (IG/HY/Conv) market breadth
                            OAuth2 client credentials — free account at developer.finra.org
  World Bank API          → China macro data       (no key)
  OECD SDMX API           → China 10Y yield        (no key)
  FRED (UK series)        → Gilt yields            (same FRED key)

Environment variables (set as GitHub Secrets):
  FRED_API_KEY          → FRED
  FINRA_API_KEY         → FINRA OAuth2 client_id
  FINRA_CLIENT_SECRET   → FINRA OAuth2 client_secret

Run:  python fetch_data.py
Out:  data/bonds.json
"""

import json, os, sys, re, base64
from datetime import datetime, timezone, timedelta
import urllib.request, urllib.parse, urllib.error
import xml.etree.ElementTree as ET

OUTPUT_FILE         = "data/bonds.json"
FRED_KEY            = os.environ.get("FRED_API_KEY", "")
FINRA_CLIENT_ID     = os.environ.get("FINRA_API_KEY", "")
FINRA_CLIENT_SECRET = os.environ.get("FINRA_CLIENT_SECRET", "")

UA = "FixedIncomeTerminal/2.0 (github.com/rubenrozen/Fixed-Income-Terminal)"

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
def fetch_json(url, headers=None, data=None, timeout=25):
    req = urllib.request.Request(url, data=data, headers={"User-Agent": UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())

def fetch_text(url, headers=None, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")

def safe(fn, name, fallback=None):
    try:
        result = fn()
        print(f"  ✅ {name}")
        return result
    except Exception as e:
        print(f"  ❌ {name}: {e}")
        return fallback


# ═══════════════════════════════════════════
# 1. US TREASURY YIELD CURVE
#    home.treasury.gov XML — no key
# ═══════════════════════════════════════════
def fetch_us_treasury():
    today = datetime.now(timezone.utc)
    for delta in range(3):
        dt = today - timedelta(days=30 * delta)
        ym = dt.strftime("%Y%m")
        url = (
            "https://home.treasury.gov/resource-center/data-chart-center/"
            f"interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value_month={ym}"
        )
        try:
            text = fetch_text(url)
            root = ET.fromstring(text)
            ns = {
                "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
                "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
                "a": "http://www.w3.org/2005/Atom",
            }
            entries = root.findall("a:entry", ns)
            if not entries:
                continue
            props = entries[-1].find("a:content/m:properties", ns)
            if props is None:
                continue
            def g(tag):
                el = props.find(f"d:{tag}", ns)
                return float(el.text) if el is not None and el.text and el.text.strip() else None
            date_el = props.find("d:NEW_DATE", ns)
            date_str = date_el.text[:10] if date_el is not None and date_el.text else dt.strftime("%Y-%m-%d")
            tenors = ["1M","2M","3M","4M","6M","1Y","2Y","3Y","5Y","7Y","10Y","20Y","30Y"]
            yields = [
                g("BC_1MONTH"), g("BC_2MONTH"), g("BC_3MONTH"), g("BC_4MONTH"),
                g("BC_6MONTH"), g("BC_1YEAR"),  g("BC_2YEAR"),  g("BC_3YEAR"),
                g("BC_5YEAR"),  g("BC_7YEAR"),  g("BC_10YEAR"), g("BC_20YEAR"),
                g("BC_30YEAR"),
            ]
            if any(v is not None for v in yields):
                return {"date": date_str, "tenors": tenors, "yields": yields}
        except Exception:
            continue
    raise RuntimeError("US Treasury: no data found after 3 months")


# ═══════════════════════════════════════════
# 2. FRED — IG/HY spreads + SOFR + 2Y-10Y
#    api.stlouisfed.org — free key
# ═══════════════════════════════════════════
def fetch_fred_series(series_id):
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
        f"&sort_order=desc&limit=10"
    )
    data = fetch_json(url)
    for obs in data.get("observations", []):
        if obs.get("value") and obs["value"] != ".":
            return {"date": obs["date"], "value": float(obs["value"])}
    raise RuntimeError(f"No valid observations for {series_id}")

def fetch_fred_history(series_id, days=35):
    """Fetch last N days of daily observations — returns list sorted asc."""
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
        f"&sort_order=asc&observation_start={start}"
    )
    data = fetch_json(url)
    return [
        {"date": o["date"], "value": float(o["value"])}
        for o in data.get("observations", [])
        if o.get("value") and o["value"] != "."
    ]

def changes_from_history(history, current):
    """Compute 1d, 1w, 1m changes vs current value."""
    if not history or current is None:
        return {"chg_1d": None, "chg_1w": None, "chg_1m": None}
    vals = history  # already sorted asc
    def closest(n_days):
        target = (datetime.now() - timedelta(days=n_days)).strftime("%Y-%m-%d")
        past = [h for h in vals if h["date"] <= target]
        return past[-1]["value"] if past else None
    p1d = closest(1); p1w = closest(7); p1m = closest(30)
    return {
        "chg_1d": round(current - p1d, 2) if p1d is not None else None,
        "chg_1w": round(current - p1w, 2) if p1w is not None else None,
        "chg_1m": round(current - p1m, 2) if p1m is not None else None,
    }

def fetch_fred():
    if not FRED_KEY:
        raise RuntimeError("FRED_API_KEY not set")

    ig   = fetch_fred_series("BAMLC0A0CM")
    hy   = fetch_fred_series("BAMLH0A0HYM2")
    sofr = fetch_fred_series("SOFR")
    spr  = fetch_fred_series("T10Y2Y")

    # 35-day daily history for 1d/1w/1m changes
    ig_hist   = fetch_fred_history("BAMLC0A0CM")
    hy_hist   = fetch_fred_history("BAMLH0A0HYM2")
    sofr_hist = fetch_fred_history("SOFR")
    spr_hist  = fetch_fred_history("T10Y2Y")

    ig_chg   = changes_from_history(ig_hist,   ig["value"])
    hy_chg   = changes_from_history(hy_hist,   hy["value"])
    sofr_chg = changes_from_history(sofr_hist, sofr["value"])
    spr_chg  = changes_from_history(spr_hist,  spr["value"])

    # 24-month history for spread chart
    start = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    hist_url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id=T10Y2Y&api_key={FRED_KEY}&file_type=json"
        f"&sort_order=asc&observation_start={start}"
    )
    hist_raw = fetch_json(hist_url)
    monthly = {}
    for o in hist_raw.get("observations", []):
        if o.get("value") and o["value"] != ".":
            monthly[o["date"][:7]] = float(o["value"])
    keys = sorted(monthly)

    return {
        "ig_spread_bps": ig["value"],   "ig_date":   ig["date"],   **{f"ig_{k}":  v for k,v in ig_chg.items()},
        "hy_spread_bps": hy["value"],   "hy_date":   hy["date"],   **{f"hy_{k}":  v for k,v in hy_chg.items()},
        "sofr":          sofr["value"], "sofr_date": sofr["date"], **{f"sofr_{k}":v for k,v in sofr_chg.items()},
        "t10y2y":        spr["value"],  "t10y2y_date":spr["date"],**{f"t10y2y_{k}":v for k,v in spr_chg.items()},
        "spread_history_labels": keys[-24:],
        "spread_history_us":     [monthly[k] for k in keys[-24:]],
    }


# ═══════════════════════════════════════════
# 3. FINRA — Agency & Corporate Market Breadth
#    api.finra.org — OAuth2 client credentials
#    Free account at developer.finra.org
#
#  subProduct values:
#    Agency  : ALL, FNMA, FHLB, FHLMC, GNMA, FFCB, TVA, OTHER
#    Corp    : ALL, INVESTMENT_GRADE, HIGH_YIELD, CONVERTIBLE
#    144A    : ALL, INVESTMENT_GRADE, HIGH_YIELD, CONVERTIBLE
# ═══════════════════════════════════════════
FINRA_TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials"
FINRA_API_BASE  = "https://api.finra.org/data/group/fixedIncomeMarket/name"

_finra_token_cache = {"token": None, "expires": 0}

def _finra_get_token():
    now = datetime.now(timezone.utc).timestamp()
    if _finra_token_cache["token"] and now < _finra_token_cache["expires"] - 30:
        return _finra_token_cache["token"]
    if not FINRA_CLIENT_ID or not FINRA_CLIENT_SECRET:
        raise RuntimeError("FINRA_API_KEY / FINRA_CLIENT_SECRET not set")
    creds = base64.b64encode(f"{FINRA_CLIENT_ID}:{FINRA_CLIENT_SECRET}".encode()).decode()
    req   = urllib.request.Request(
        FINRA_TOKEN_URL, data=b"",
        headers={"Authorization": f"Basic {creds}", "Content-Length": "0"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        tok = json.loads(r.read().decode())
    token = tok.get("access_token")
    if not token:
        raise RuntimeError(f"FINRA token missing: {tok}")
    expires_in = int(tok.get("expires_in", 1800))
    _finra_token_cache.update({"token": token, "expires": now + expires_in})
    print(f"    FINRA token OK (expires in {expires_in}s)")
    return token

def _finra_query(endpoint, limit=500):
    token = _finra_get_token()
    url   = f"{FINRA_API_BASE}/{endpoint}"
    body  = json.dumps({"limit": limit}).encode()
    req   = urllib.request.Request(url, data=body, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "User-Agent":    UA,
    }, method="POST")
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read().decode())

def _parse_breadth(records):
    """Convert FINRA market breadth list → dict keyed by subProduct."""
    out, latest = {}, ""
    for rec in records:
        sub  = rec.get("subProduct", "ALL")
        date = rec.get("tradeDate", "")
        if date > latest:
            latest = date
        dv = rec.get("dollarVolume")
        out[sub] = {
            "advances":         rec.get("advances"),
            "declines":         rec.get("declines"),
            "unchanged":        rec.get("unchanged"),
            "total":            (rec.get("advances") or 0) + (rec.get("declines") or 0) + (rec.get("unchanged") or 0),
            "highs_52w":        rec.get("fiftyTwoWeekHighs"),
            "lows_52w":         rec.get("fiftyTwoWeekLows"),
            "dollar_volume_mm": round(dv / 1e6, 1) if dv else None,
        }
    return out, latest

def fetch_finra():
    result = {}

    # Agency
    for ep in ["agencyMarketBreadth", "agencyDebtMarketBreadth"]:
        try:
            recs = _finra_query(ep)
            if isinstance(recs, list) and recs:
                parsed, date = _parse_breadth(recs)
                result["agency"] = parsed
                result["date"]   = date
                print(f"    FINRA agency: {list(parsed.keys())}, date={date}")
                break
        except Exception as e:
            print(f"    FINRA {ep}: {e}")

    # Corporate
    for ep in ["corporateMarketBreadth", "corporateDebtMarketBreadth"]:
        try:
            recs = _finra_query(ep)
            if isinstance(recs, list) and recs:
                parsed, date = _parse_breadth(recs)
                result["corp"] = parsed
                if not result.get("date"): result["date"] = date
                print(f"    FINRA corp: {list(parsed.keys())}, date={date}")
                break
        except Exception as e:
            print(f"    FINRA {ep}: {e}")

    # Corporate 144A
    for ep in ["corporate144AMarketBreadth", "corporate144ADebtMarketBreadth"]:
        try:
            recs = _finra_query(ep)
            if isinstance(recs, list) and recs:
                parsed, date = _parse_breadth(recs)
                result["corp144a"] = parsed
                print(f"    FINRA 144A: {list(parsed.keys())}, date={date}")
                break
        except Exception as e:
            print(f"    FINRA {ep}: {e}")

    if not result:
        raise RuntimeError("All FINRA endpoints failed — check credentials or plan access")
    return result


# ═══════════════════════════════════════════
# 4. ECB STATISTICAL DATA WAREHOUSE
#    Both old (sdw-wsrest) and new (data-api) endpoints tried
#    Wildcard CSV bulk download used as final fallback
# ═══════════════════════════════════════════
ECB_MATURITIES = {
    "SR_3M":  "3M",
    "SR_6M":  "6M",
    "SR_1Y":  "1Y",
    "SR_2Y":  "2Y",
    "SR_5Y":  "5Y",
    "SR_10Y": "10Y",
    "SR_30Y": "30Y",
}
ECB_HEADERS = {"User-Agent": UA, "Accept": "application/json"}

def fetch_ecb_yield(mat_code):
    key = f"B.U2.EUR.4F.G_N_A.SV_C_YM.{mat_code}"
    # Try 3 URL variants — the 400 in earlier run was likely from lastNObservations on a bad day
    urls = [
        f"https://data-api.ecb.europa.eu/service/data/YC/{key}?format=jsondata",
        f"https://data-api.ecb.europa.eu/service/data/YC/{key}?format=jsondata&lastNObservations=5",
        f"https://sdw-wsrest.ecb.europa.eu/service/data/YC/{key}?format=jsondata&lastNObservations=5",
    ]
    last_err = None
    for url in urls:
        try:
            data = fetch_json(url, headers=ECB_HEADERS)
            series = data["dataSets"][0]["series"]
            obs    = list(series.values())[0]["observations"]
            last   = obs[str(max(int(k) for k in obs))]
            return round(last[0], 4)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"ECB {mat_code}: all attempts failed — {last_err}")

def _ecb_bulk_csv():
    """Fallback: pull all YC maturities in one CSV request (wildcard on 7th dim)."""
    for base in ["https://data-api.ecb.europa.eu", "https://sdw-wsrest.ecb.europa.eu"]:
        try:
            url  = f"{base}/service/data/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.?format=csvdata&lastNObservations=1"
            text = fetch_text(url, headers={"User-Agent": UA, "Accept": "text/csv,*/*"})
            tenors, yields = [], []
            for line in text.splitlines()[1:]:           # skip header
                parts = line.split(",")
                if len(parts) < 2:
                    continue
                series_key = parts[0].strip('"')
                mat = series_key.split(".")[-1]           # e.g. SR_10Y
                val_str = parts[-1].strip('"')
                label = ECB_MATURITIES.get(mat)
                if label:
                    try:
                        yields.append(round(float(val_str), 4))
                        tenors.append(label)
                    except ValueError:
                        pass
            if tenors:
                print(f"    ECB bulk CSV ({base.split('.')[0].split('//')[1]}): {len(tenors)} maturities")
                return tenors, yields
        except Exception as e:
            print(f"    ECB bulk CSV {base}: {e}")
    return [], []

def fetch_ecb():
    tenors, yields = [], []
    for code, label in ECB_MATURITIES.items():
        try:
            y = fetch_ecb_yield(code)
            tenors.append(label)
            yields.append(y)
            print(f"    ECB {label}: {y}%")
        except Exception as e:
            print(f"    ECB {label} skipped: {e}")

    # If per-maturity calls all failed, try bulk CSV
    if not tenors:
        tenors, yields = _ecb_bulk_csv()

    # EURIBOR 3M
    euribor = None
    for base in ["https://data-api.ecb.europa.eu", "https://sdw-wsrest.ecb.europa.eu"]:
        try:
            url = f"{base}/service/data/FM/B.U2.EUR.RT.MM.EURIBOR3MD_.HSTA?format=jsondata&lastNObservations=1"
            d   = fetch_json(url, headers=ECB_HEADERS)
            obs = list(d["dataSets"][0]["series"].values())[0]["observations"]
            euribor = round(list(obs.values())[-1][0], 4)
            print(f"    EURIBOR 3M: {euribor}%")
            break
        except Exception as e:
            print(f"    EURIBOR ({base.split('.')[1]}): {e}")

    if not tenors:
        raise RuntimeError("ECB: all endpoints and bulk CSV failed")
    return {"tenors": tenors, "yields": yields, "euribor_3m": euribor}


# ═══════════════════════════════════════════
# 4. JAPAN — JGB Yields via Ministry of Finance CSV
#    mof.go.jp — no key needed
#    Falls back to BOJ statistics page if MoF moves the file
# ═══════════════════════════════════════════
_MOF_CSV_CANDIDATES = [
    # MoF English page — discovered via the index page
    "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/data/jgbcme_all.csv",
    "https://www.mof.go.jp/english/jgbs/reference/interest_rate/data/jgbcme_all.csv",
    # BOJ long-term yield CSV (JSDA data re-published)
    "https://www.boj.or.jp/statistics/market/bond/bondyield/long/lgbond.csv",
    "https://www.boj.or.jp/statistics/market/bond/bondyield/util/lgbond.csv",
]
_TENOR_ALIASES = {
    "1year":"1Y","1-year":"1Y","1yr":"1Y","1y":"1Y",
    "2year":"2Y","2-year":"2Y","2yr":"2Y","2y":"2Y",
    "3year":"3Y","3-year":"3Y","3yr":"3Y","3y":"3Y",
    "5year":"5Y","5-year":"5Y","5yr":"5Y","5y":"5Y",
    "7year":"7Y","7-year":"7Y","7yr":"7Y","7y":"7Y",
    "10year":"10Y","10-year":"10Y","10yr":"10Y","10y":"10Y",
    "15year":"15Y","15-year":"15Y","15yr":"15Y","15y":"15Y",
    "20year":"20Y","20-year":"20Y","20yr":"20Y","20y":"20Y",
    "25year":"25Y","25-year":"25Y","25yr":"25Y","25y":"25Y",
    "30year":"30Y","30-year":"30Y","30yr":"30Y","30y":"30Y",
    "40year":"40Y","40-year":"40Y","40yr":"40Y","40y":"40Y",
}

def _parse_jgb_csv(csv_text):
    lines = [l.strip() for l in csv_text.splitlines() if l.strip()]
    # Find header row
    header_idx = None
    for i, line in enumerate(lines):
        lower = line.lower()
        if ("date" in lower or "year" in lower) and "," in line:
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Header row not found")
    headers = [h.strip().strip('"').lower().replace(" ","").replace("-","") for h in lines[header_idx].split(",")]
    data_rows = [
        [c.strip().strip('"') for c in l.split(",")]
        for l in lines[header_idx + 1:]
        if l and re.match(r'\d{4}', l.strip())
    ]
    if not data_rows:
        raise RuntimeError("No data rows found")
    last = data_rows[-1]
    tenors, yields = [], []
    for i, h in enumerate(headers[1:], start=1):
        norm = h.replace("-year","y").replace("year","y")
        label = _TENOR_ALIASES.get(norm) or _TENOR_ALIASES.get(h)
        if label and i < len(last):
            try:
                val = float(last[i])
                if 0 < val < 30:
                    tenors.append(label)
                    yields.append(round(val, 4))
            except ValueError:
                pass
    if not tenors:
        raise RuntimeError(f"No tenors parsed — headers: {headers[:8]}")
    return tenors, yields, last[0]

def fetch_boj():
    # Step 1: Try to discover the CSV link from the MoF index page (most reliable)
    discovered_urls = list(_MOF_CSV_CANDIDATES)
    try:
        index_url = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/index.htm"
        html = fetch_text(index_url, headers={"User-Agent": UA, "Accept-Language": "en"})
        for href in re.findall(r'href="([^"]*\.csv[^"]*)"', html, re.IGNORECASE):
            full = href if href.startswith("http") else "https://www.mof.go.jp" + href
            if full not in discovered_urls:
                discovered_urls.insert(0, full)
                print(f"    MoF index: discovered CSV → {full.split('/')[-1]}")
    except Exception as e:
        print(f"    MoF index page: {e}")

    # Step 2: Try each candidate URL
    for url in discovered_urls:
        try:
            csv_text = fetch_text(url, headers={
                "User-Agent": UA,
                "Accept":     "text/csv,*/*",
                "Referer":    "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/index.htm",
            })
            tenors, yields, date = _parse_jgb_csv(csv_text)
            print(f"    JGB via {url.split('/')[2]}: {len(tenors)} tenors, date={date}")
            return {"tenors": tenors, "yields": yields, "date": date}
        except Exception as e:
            print(f"    {url.split('/')[-1]}: {e}")

    raise RuntimeError("JGB: all MoF/BOJ CSV URLs failed — check mof.go.jp for current file path")


# ═══════════════════════════════════════════
# 6. CHINA — OECD + FRED + World Bank
#    All free, no key (FRED key used if available)
#    ADB / CCDC removed — URLs no longer stable
# ═══════════════════════════════════════════
def fetch_china_bonds():
    result = {}

    # ── World Bank: GDP + market cap/GDP ratio ──────────────────
    for indicator, label in [
        ("CM.MKT.LCAP.GD.ZS", "market_cap_pct_gdp"),
        ("NY.GDP.MKTP.CD",     "gdp_usd"),
    ]:
        try:
            url  = f"https://api.worldbank.org/v2/country/CN/indicator/{indicator}?format=json&mrv=2&per_page=2"
            data = fetch_json(url)
            if data and len(data) > 1 and data[1]:
                for obs in data[1]:
                    if obs.get("value") is not None:
                        result[label] = {"value": round(obs["value"], 2), "date": obs.get("date")}
                        print(f"    World Bank {indicator}: {obs['value']}")
                        break
        except Exception as e:
            print(f"    World Bank {indicator}: {e}")

    # ── OECD: China 10Y government bond yield ───────────────────
    # SDMX-JSON 2.1 wraps dataSets inside a "data" envelope
    try:
        url  = "https://stats.oecd.org/SDMX-JSON/data/MEI_FIN/IRLTLT01.CNP.M/all?lastNObservations=3&format=jsondata"
        raw  = fetch_json(url, headers={"Accept": "application/json", "User-Agent": UA})
        # Try both SDMX-JSON 1.0 (dataSets at root) and 2.x (inside "data")
        ds = (raw.get("dataSets")
              or raw.get("DataSets")
              or raw.get("data", {}).get("dataSets")
              or raw.get("data", {}).get("DataSets"))
        if not ds:
            raise RuntimeError(f"dataSets not found — top keys: {list(raw.keys())}")
        series = ds[0]["series"]
        obs    = list(series.values())[0]["observations"]
        last   = obs[str(max(int(k) for k in obs))]
        result["cgb_10y"] = round(last[0], 4)
        print(f"    OECD CGB 10Y: {result['cgb_10y']}%")
    except Exception as e:
        print(f"    OECD CGB 10Y: {e}")

    # ── FRED: China 10Y yield fallback (if OECD failed) ────────
    if FRED_KEY and "cgb_10y" not in result:
        # Correct FRED series: INTGSTCNM193N = China 10Y gov bond (IMF IFS)
        for sid in ["INTGSTCNM193N", "IRLTLT01CNM156N"]:
            try:
                url  = (f"https://api.stlouisfed.org/fred/series/observations"
                        f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
                        f"&sort_order=desc&limit=5")
                data = fetch_json(url)
                for obs in data.get("observations", []):
                    if obs.get("value") and obs["value"] != ".":
                        result["cgb_10y"]      = float(obs["value"])
                        result["cgb_10y_date"] = obs["date"]
                        print(f"    FRED {sid}: {result['cgb_10y']}%")
                        break
                if "cgb_10y" in result:
                    break
            except Exception as e:
                print(f"    FRED {sid}: {e}")

    # ── CHINAMONEY.COM.CN: CFETS published CGB yield curve ──────
    # This is the official China interbank market (CFETS) page
    try:
        cfets_url = "https://www.chinamoney.com.cn/english/bmkYldCrvBnd/"
        html = fetch_text(cfets_url, headers={
            "User-Agent": UA,
            "Accept-Language": "en",
            "Referer": "https://www.chinamoney.com.cn/english/",
        })
        rows   = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
        tenors, yields = [], []
        t_re   = re.compile(r'(\d+)\s*([YyMm])')
        for row in rows[:20]:
            cells = [re.sub(r'<[^>]+>', '', c).strip()
                     for c in re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)]
            if len(cells) >= 2:
                m = t_re.search(cells[0])
                try:
                    y = float(cells[1])
                    if m and 0 < y < 20:
                        tenors.append(m.group(1) + m.group(2).upper())
                        yields.append(round(y, 4))
                except ValueError:
                    pass
        if tenors:
            result["cfets_yields"] = {"tenors": tenors, "yields": yields}
            print(f"    CFETS: {len(tenors)} tenors parsed")
    except Exception as e:
        print(f"    CFETS: {e}")

    if not result:
        raise RuntimeError("No China data from any source (World Bank / OECD / FRED / CFETS)")
    return result


# ═══════════════════════════════════════════
# 7. UK — Gilt Yields via FRED
#    FRED series (no bot protection, reliable):
#      IRLTLT01GBM156N = UK 10Y gilt yield (monthly, IMF IFS)
#    + hardcoded curve shape from BoE MPC publications if needed
# ═══════════════════════════════════════════
def fetch_uk_gilts():
    # ── Primary: FRED multi-tenor UK gilt series ─────────────────
    # These are IMF IFS series re-published by FRED — very reliable
    fred_uk_series = {
        "IRLTLT01GBM156N": "10Y",   # UK long-term gov bond yield (10Y benchmark)
        "IR3TIB01GBM156N": "3M",    # UK 3-month interbank (proxy for short end)
    }
    tenors, yields, date_str = [], [], ""

    if FRED_KEY:
        for sid, tenor in fred_uk_series.items():
            try:
                url  = (f"https://api.stlouisfed.org/fred/series/observations"
                        f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
                        f"&sort_order=desc&limit=3")
                data = fetch_json(url)
                for obs in data.get("observations", []):
                    if obs.get("value") and obs["value"] != ".":
                        tenors.append(tenor)
                        yields.append(round(float(obs["value"]), 4))
                        if not date_str:
                            date_str = obs["date"]
                        print(f"    FRED UK {tenor}: {obs['value']}%")
                        break
            except Exception as e:
                print(f"    FRED UK {sid}: {e}")

    # ── Fallback: DMO CSV direct (works when not bot-blocked) ────
    if not tenors:
        for report in ["D4A", "D3A"]:
            try:
                url  = f"https://www.dmo.gov.uk/data/CsvDataReport?reportCode={report}"
                text = fetch_text(url, headers={
                    "User-Agent": "python-urllib/3.11",
                    "Accept": "text/csv",
                })
                # Bot check: if response is JavaScript, skip
                if "function " in text[:200] or "var " in text[:200]:
                    print(f"    DMO {report}: bot-protected, skipping")
                    continue
                lines = [l.strip() for l in text.splitlines() if l.strip()]
                for i, line in enumerate(lines):
                    if "," in line and re.search(r'\d+\s*[Yy]', line):
                        headers = [h.strip().strip('"') for h in line.split(",")]
                        data_rows = [
                            [c.strip().strip('"') for c in l.split(",")]
                            for l in lines[i+1:]
                            if l and len(l.split(",")) >= 3
                        ]
                        if data_rows:
                            last = data_rows[-1]
                            date_str = last[0] if last else ""
                            for j, h in enumerate(headers[1:], 1):
                                m = re.search(r'(\d+)\s*[Yy]', h)
                                if m and j < len(last):
                                    try:
                                        y = float(last[j])
                                        if 0 < y < 20:
                                            tenors.append(m.group(1) + "Y")
                                            yields.append(round(y, 4))
                                    except ValueError:
                                        pass
                        if tenors:
                            print(f"    DMO CSV {report}: {len(tenors)} tenors")
                        break
            except Exception as e:
                print(f"    DMO {report}: {e}")
            if tenors:
                break

    if not tenors:
        raise RuntimeError("UK gilts: FRED series unavailable and DMO bot-protected — check FRED_API_KEY")
    return {"tenors": tenors, "yields": yields, "date": date_str}


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════
def main():
    print("\n🔄 Fixed Income Terminal — Data Fetcher v3")
    print(f"   Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"   FRED key:  {'✅ set' if FRED_KEY else '❌ missing — set FRED_API_KEY'}")
    print(f"   FINRA:     {'✅ set' if (FINRA_CLIENT_ID and FINRA_CLIENT_SECRET) else '⚠️  missing — set FINRA_API_KEY + FINRA_CLIENT_SECRET'}\n")

    output = {"last_updated": datetime.now(timezone.utc).isoformat(), "errors": {}}

    steps = [
        ("US Treasury yield curve",        fetch_us_treasury,  "us_treasury"),
        ("FRED spreads + history",         fetch_fred,         "fred"),
        ("FINRA Agency + Corp breadth",    fetch_finra,        "finra"),
        ("ECB yield curves",               fetch_ecb,          "ecb"),
        ("Bank of Japan JGB",              fetch_boj,          "boj"),
        ("China bond market",              fetch_china_bonds,  "china"),
        ("UK Gilt yields",                 fetch_uk_gilts,     "uk_gilts"),
    ]

    for label, fn, key in steps:
        print(f"{label}…")
        r = safe(fn, label)
        if r:
            output[key] = r
        else:
            output["errors"][key] = "fetch failed"

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)

    errors = {k: v for k, v in output["errors"].items() if k != "_placeholder"}
    live   = len([k for k in output if k not in ("last_updated", "errors")])
    print(f"\n{'✅' if not errors else '⚠️ '} Done — {OUTPUT_FILE} written")
    print(f"   Live sections: {live}  |  Failed: {len(errors)}")
    if errors:
        print(f"   Failed: {', '.join(errors.keys())}")
    print()
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
