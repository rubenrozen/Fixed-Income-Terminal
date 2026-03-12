"""
Fixed Income Terminal — Data Fetcher
=====================================
Sources (all free, no paid subscription):
  - US Treasury XML feed         → US yield curve (no key)
  - FRED API                     → IG/HY spreads, SOFR (free key)
  - ECB Statistical Data Warehouse → EU/Bund curves (no key)
  - Bank of Japan API            → JGB yields (no key)
  - FINRA Market Data API        → US Agency + Corp volumes (free key)
  - ADB Asian Bonds Online       → China bond market data (no key)

Required environment variables (set as GitHub Secrets):
  FRED_API_KEY    → get free at fred.stlouisfed.org/docs/api/api_key.html
  FINRA_API_KEY   → get free at developer.finra.org

Run:  python fetch_data.py
Output: data/bonds.json
"""

import json
import os
import sys
import traceback
from datetime import datetime, timezone, timedelta
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET

OUTPUT_FILE = "data/bonds.json"
FRED_KEY    = os.environ.get("FRED_API_KEY", "")
FINRA_KEY   = os.environ.get("FINRA_API_KEY", "")

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
def fetch_json(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {
        "User-Agent": "FixedIncomeTerminal/1.0 (github.com/rubenrozen/Fixed-Income-Terminal)"
    })
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())

def fetch_text(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {
        "User-Agent": "FixedIncomeTerminal/1.0"
    })
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode()

def safe(fn, name, fallback=None):
    try:
        result = fn()
        print(f"  ✅ {name}")
        return result
    except Exception as e:
        print(f"  ❌ {name}: {e}")
        return fallback

# ─────────────────────────────────────────
# 1. US TREASURY YIELD CURVE
#    Source: home.treasury.gov XML feed (no key)
#    Frequency: daily
# ─────────────────────────────────────────
def fetch_us_treasury():
    today = datetime.now(timezone.utc)
    # Try current month, fall back to previous if no data yet
    for delta in range(0, 3):
        dt = today - timedelta(days=30 * delta)
        year_month = dt.strftime("%Y%m")
        url = (
            f"https://home.treasury.gov/resource-center/data-chart-center/"
            f"interest-rates/pages/xml?data=daily_treasury_yield_curve"
            f"&field_tdr_date_value_month={year_month}"
        )
        try:
            text = fetch_text(url)
            root = ET.fromstring(text)
            ns = {
                "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
                "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
                "a": "http://www.w3.org/2005/Atom",
            }
            # Get last entry (most recent date)
            entries = root.findall("a:entry", ns)
            if not entries:
                continue
            props = entries[-1].find("a:content/m:properties", ns)
            if props is None:
                continue
            def g(tag):
                el = props.find(f"d:{tag}", ns)
                return float(el.text) if el is not None and el.text else None
            date_el = props.find("d:NEW_DATE", ns)
            date_str = date_el.text[:10] if date_el is not None else dt.strftime("%Y-%m-%d")
            return {
                "date": date_str,
                "tenors": ["1M","2M","3M","4M","6M","1Y","2Y","3Y","5Y","7Y","10Y","20Y","30Y"],
                "yields": [
                    g("BC_1MONTH"), g("BC_2MONTH"), g("BC_3MONTH"), g("BC_4MONTH"),
                    g("BC_6MONTH"), g("BC_1YEAR"),  g("BC_2YEAR"),  g("BC_3YEAR"),
                    g("BC_5YEAR"),  g("BC_7YEAR"),  g("BC_10YEAR"), g("BC_20YEAR"),
                    g("BC_30YEAR"),
                ],
            }
        except Exception:
            continue
    raise RuntimeError("US Treasury: no data found")

# ─────────────────────────────────────────
# 2. FRED — IG/HY Credit Spreads + SOFR
#    Source: api.stlouisfed.org (free key)
#    Series used:
#      BAMLC0A0CM   → US IG OAS spread (bps)
#      BAMLH0A0HYM2 → US HY OAS spread (bps)
#      SOFR         → Secured Overnight Financing Rate
#      T10Y2Y       → 10Y-2Y spread
# ─────────────────────────────────────────
def fetch_fred_series(series_id):
    if not FRED_KEY:
        raise RuntimeError("FRED_API_KEY not set")
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
        f"&sort_order=desc&limit=30"
    )
    data = fetch_json(url)
    # Return last non-null observation
    for obs in data.get("observations", []):
        if obs.get("value") and obs["value"] != ".":
            return {"date": obs["date"], "value": float(obs["value"])}
    raise RuntimeError(f"No data for {series_id}")

def fetch_fred():
    ig    = fetch_fred_series("BAMLC0A0CM")
    hy    = fetch_fred_series("BAMLH0A0HYM2")
    sofr  = fetch_fred_series("SOFR")
    spr   = fetch_fred_series("T10Y2Y")
    # Historical 2Y-10Y spread (24 months for the chart)
    if not FRED_KEY:
        raise RuntimeError("FRED_API_KEY not set")
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id=T10Y2Y&api_key={FRED_KEY}&file_type=json"
        f"&sort_order=asc&limit=500&observation_start="
        f"{(datetime.now()-timedelta(days=730)).strftime('%Y-%m-%d')}"
    )
    hist_data = fetch_json(url)
    hist = [
        {"date": o["date"], "value": float(o["value"])}
        for o in hist_data.get("observations", [])
        if o.get("value") and o["value"] != "."
    ]
    # Monthly sample for chart
    monthly = {}
    for h in hist:
        key = h["date"][:7]  # YYYY-MM
        monthly[key] = h["value"]
    sorted_months = sorted(monthly.keys())
    return {
        "ig_spread_bps":  ig["value"],
        "ig_date":        ig["date"],
        "hy_spread_bps":  hy["value"],
        "hy_date":        hy["date"],
        "sofr":           sofr["value"],
        "sofr_date":      sofr["date"],
        "t10y2y":         spr["value"],
        "t10y2y_date":    spr["date"],
        "spread_history_labels": sorted_months[-24:],
        "spread_history_us":     [monthly[k] for k in sorted_months[-24:]],
    }

# ─────────────────────────────────────────
# 3. ECB STATISTICAL DATA WAREHOUSE
#    Source: data-api.ecb.europa.eu (no key)
#    Series:
#      YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_X → AAA EU yield curve spot rates
#      FM.B.U2.EUR.RT.MM.EURIBOR3MD_.HSTA → EURIBOR 3M
# ─────────────────────────────────────────
def fetch_ecb_series(flow, key):
    url = f"https://data-api.ecb.europa.eu/service/data/{flow}/{key}?format=jsondata&lastNObservations=1"
    data = fetch_json(url, headers={
        "User-Agent": "FixedIncomeTerminal/1.0",
        "Accept": "application/json",
    })
    series = data["dataSets"][0]["series"]
    first_series = list(series.values())[0]
    obs = first_series["observations"]
    last_obs = obs[max(obs.keys(), key=int)]
    return last_obs[0]

def fetch_ecb():
    # Maturity codes in ECB yield curve dataset
    # P1Y=1Y, P2Y=2Y, P5Y=5Y, P7Y=7Y, P10Y=10Y
    maturity_map = {
        "P3M": "3M", "P6M": "6M", "P1Y": "1Y",
        "P2Y": "2Y", "P5Y": "5Y", "P10Y": "10Y", "P30Y": "30Y",
    }
    tenors = []
    yields = []
    for ecb_code, label in maturity_map.items():
        try:
            key = f"YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_{ecb_code}"
            flow = "YC"
            url = f"https://data-api.ecb.europa.eu/service/data/{flow}/{key}?format=jsondata&lastNObservations=1"
            data = fetch_json(url, headers={"User-Agent": "FixedIncomeTerminal/1.0", "Accept": "application/json"})
            s = data["dataSets"][0]["series"]
            first = list(s.values())[0]
            obs = first["observations"]
            val = obs[max(obs.keys(), key=int)][0]
            tenors.append(label)
            yields.append(round(val, 4))
        except Exception as e:
            print(f"    ECB {label}: {e}")
    # EURIBOR 3M
    try:
        euribor = fetch_ecb_series("FM", "B.U2.EUR.RT.MM.EURIBOR3MD_.HSTA")
    except Exception:
        euribor = None
    return {"tenors": tenors, "yields": yields, "euribor_3m": euribor}

# ─────────────────────────────────────────
# 4. BANK OF JAPAN — JGB Yields
#    Source: stat-search.boj.or.jp (no key)
#    Series IR01 = JGB market yields
# ─────────────────────────────────────────
def fetch_boj():
    # BOJ provides CSV data via their statistics search
    # Series: Interest Rates > JGB Yields
    url = "https://www.stat-search.boj.or.jp/ssi/mtsutil/rest/getGraph/IR01?output=CSV"
    try:
        text = fetch_text(url)
        # Parse CSV — format varies, extract latest row
        lines = [l.strip() for l in text.split("\n") if l.strip() and not l.startswith("#")]
        headers_line = None
        data_lines = []
        for l in lines:
            if "年月" in l or "Date" in l or "date" in l.lower():
                headers_line = l.split(",")
            elif headers_line and l[0].isdigit():
                data_lines.append(l.split(","))
        if not data_lines:
            raise RuntimeError("No data rows")
        last = data_lines[-1]
        # Map: 2Y, 5Y, 10Y, 20Y, 30Y typically
        tenors = ["2Y","5Y","10Y","20Y","30Y"]
        yields = []
        for i, v in enumerate(last[1:6]):
            try:
                yields.append(round(float(v), 4))
            except Exception:
                yields.append(None)
        return {"tenors": tenors, "yields": yields, "date": last[0]}
    except Exception as e:
        # Fallback: use a simpler BOJ endpoint
        raise RuntimeError(f"BOJ fetch failed: {e}")

# ─────────────────────────────────────────
# 5. FINRA MARKET DATA API
#    Source: api.finra.org (free key from developer.finra.org)
#    Endpoint: /data/group/otcMarket/name/weeklySummary
#    Covers: Agency, Corporate, ABS weekly volume
# ─────────────────────────────────────────
def fetch_finra():
    if not FINRA_KEY:
        raise RuntimeError("FINRA_API_KEY not set")

    headers = {
        "Authorization": f"Bearer {FINRA_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "FixedIncomeTerminal/1.0",
    }

    # Weekly OTC bond market summary
    url = "https://api.finra.org/data/group/otcMarket/name/weeklySummary"
    data = fetch_json(url, headers=headers)

    # Parse into our schema
    result = {
        "agency": {},
        "corp_ig": {},
        "corp_hy": {},
        "date": None,
    }

    for row in data:
        market = row.get("market", "")
        rtype  = row.get("reportType", "")
        dt     = row.get("reportDate", "")
        if not result["date"]:
            result["date"] = dt

        vol_buy  = row.get("buyVolume", 0)
        vol_sell = row.get("sellVolume", 0)
        vol_idlr = row.get("interDealerVolume", 0)
        trades   = row.get("tradeCount", 0)

        entry = {
            "buy_volume_mm":      round(float(vol_buy)  / 1e6, 1) if vol_buy  else None,
            "sell_volume_mm":     round(float(vol_sell) / 1e6, 1) if vol_sell else None,
            "interdealer_vol_mm": round(float(vol_idlr) / 1e6, 1) if vol_idlr else None,
            "trade_count":        trades,
        }

        if "Agency" in market or "agency" in market.lower():
            result["agency"] = entry
        elif "Investment Grade" in rtype or "IG" in rtype:
            result["corp_ig"] = entry
        elif "High Yield" in rtype or "HY" in rtype:
            result["corp_hy"] = entry

    # Also fetch TRACE aggregate for specific issuers (Agency breakdown)
    try:
        issuer_url = "https://api.finra.org/data/group/otcMarket/name/agencyIssuerSummary"
        issuer_data = fetch_json(issuer_url, headers=headers)
        issuers = {}
        for row in issuer_data:
            name = row.get("issuerName", "")
            vol  = row.get("totalVolume", 0)
            if name:
                issuers[name] = round(float(vol) / 1e6, 1) if vol else None
        result["agency_issuers"] = issuers
    except Exception:
        result["agency_issuers"] = {}

    return result

# ─────────────────────────────────────────
# 6. CHINA BONDS — ADB Asian Bonds Online
#    Source: asianbondsonline.adb.org (no key)
#    + CCDC chinabond.com.cn (public weekly stats)
#    Covers: CGB yields, bond market size, weekly volume
# ─────────────────────────────────────────
def fetch_china_bonds():
    result = {}

    # ADB Asian Bonds Online — China bond market indicators
    # They expose data files at a consistent URL pattern
    try:
        # Yield data — government bond yields by maturity
        adb_url = "https://asianbondsonline.adb.org/api/data/cn/yield"
        adb_data = fetch_json(adb_url, headers={
            "User-Agent": "FixedIncomeTerminal/1.0",
            "Accept": "application/json",
        })
        if adb_data:
            result["adb_yield"] = adb_data
    except Exception as e:
        print(f"    ADB yield: {e}")

    # ADB — bond market size outstanding
    try:
        size_url = "https://asianbondsonline.adb.org/api/data/cn/marketsize"
        size_data = fetch_json(size_url, headers={"User-Agent": "FixedIncomeTerminal/1.0"})
        if size_data:
            result["adb_size"] = size_data
    except Exception as e:
        print(f"    ADB size: {e}")

    # CCDC (chinabond.com.cn) weekly bond statistics
    # They publish weekly turnover stats as HTML table, we parse it
    try:
        ccdc_url = "https://www.chinabond.com.cn/cb/cn/sjtj/zqsc/scjy/index.shtml"
        html = fetch_text(ccdc_url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; FixedIncomeTerminal/1.0)",
            "Accept": "text/html",
        })
        # Extract first data table values — crude but functional
        import re
        # Look for numbers in context of bond market data
        tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL)
        if tables:
            result["ccdc_raw_table_count"] = len(tables)
            # Extract numeric data from first meaningful table
            numbers = re.findall(r'>([\d,\.]+)<', tables[0])
            result["ccdc_raw_numbers"] = numbers[:20]
    except Exception as e:
        print(f"    CCDC: {e}")

    # World Bank — China bond market GDP ratio (for context)
    try:
        wb_url = "https://api.worldbank.org/v2/country/CN/indicator/CM.MKT.LCAP.GD.ZS?format=json&mrv=1"
        wb_data = fetch_json(wb_url)
        if wb_data and len(wb_data) > 1 and wb_data[1]:
            result["wb_market_cap_gdp"] = wb_data[1][0]
    except Exception as e:
        print(f"    WorldBank: {e}")

    if not result:
        raise RuntimeError("No China data retrieved from any source")
    return result

# ─────────────────────────────────────────
# 7. MULTI-COUNTRY GOV BOND YIELDS
#    Sources: US Treasury + ECB + BOJ + UK DMO
# ─────────────────────────────────────────
def fetch_uk_gilts():
    # UK DMO publishes gilt yields as CSV
    # https://www.dmo.gov.uk/data/pdfdatareport?reportCode=D4A
    url = "https://www.dmo.gov.uk/data/XmlDataReport?reportCode=D4A"
    try:
        text = fetch_text(url)
        root = ET.fromstring(text)
        # Parse UK DMO XML
        records = root.findall(".//ISDAGILTYIELD") or root.findall(".//row") or root.findall(".//{*}row")
        if records:
            last = records[-1]
            tenors, yields = [], []
            for child in last:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                try:
                    y = float(child.text)
                    # Map tag to tenor if recognizable
                    if "2Y" in tag or "2y" in tag:  tenors.append("2Y"); yields.append(y)
                    elif "5Y" in tag or "5y" in tag: tenors.append("5Y"); yields.append(y)
                    elif "10Y" in tag or "10y" in tag: tenors.append("10Y"); yields.append(y)
                    elif "20Y" in tag or "20y" in tag: tenors.append("20Y"); yields.append(y)
                    elif "30Y" in tag or "30y" in tag: tenors.append("30Y"); yields.append(y)
                except (ValueError, TypeError):
                    pass
            return {"tenors": tenors, "yields": yields}
    except Exception as e:
        raise RuntimeError(f"UK gilts: {e}")

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    print("\n🔄 Fixed Income Terminal — Fetching live data")
    print(f"   Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"   FRED key:  {'✅ set' if FRED_KEY else '❌ missing (set FRED_API_KEY)'}")
    print(f"   FINRA key: {'✅ set' if FINRA_KEY else '❌ missing (set FINRA_API_KEY)'}\n")

    output = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "sources": {},
        "errors":  {},
    }

    print("US Treasury yield curve…")
    r = safe(fetch_us_treasury, "US Treasury curve")
    if r: output["us_treasury"]  = r
    else: output["errors"]["us_treasury"] = "fetch failed"

    print("FRED credit spreads…")
    r = safe(fetch_fred, "FRED (spreads + history)")
    if r: output["fred"] = r
    else: output["errors"]["fred"] = "fetch failed — check FRED_API_KEY"

    print("ECB yield curves…")
    r = safe(fetch_ecb, "ECB SDW (EU curves)")
    if r: output["ecb"] = r
    else: output["errors"]["ecb"] = "fetch failed"

    print("Bank of Japan JGB…")
    r = safe(fetch_boj, "BOJ (JGB yields)")
    if r: output["boj"] = r
    else: output["errors"]["boj"] = "fetch failed"

    print("FINRA TRACE volumes…")
    r = safe(fetch_finra, "FINRA API (bond volumes)")
    if r: output["finra"] = r
    else: output["errors"]["finra"] = "fetch failed — check FINRA_API_KEY"

    print("China bond market…")
    r = safe(fetch_china_bonds, "China bonds (ADB + CCDC)")
    if r: output["china"] = r
    else: output["errors"]["china"] = "fetch failed"

    print("UK Gilt yields…")
    r = safe(fetch_uk_gilts, "UK DMO (Gilts)")
    if r: output["uk_gilts"] = r
    else: output["errors"]["uk_gilts"] = "fetch failed"

    # Write output
    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)

    errors = output.get("errors", {})
    print(f"\n{'✅' if not errors else '⚠️ '} Done — {OUTPUT_FILE} written")
    if errors:
        print(f"   {len(errors)} source(s) failed: {', '.join(errors.keys())}")
    print(f"   Sections with live data: {len([k for k in output if k not in ('last_updated','sources','errors')])}\n")
    return 0 if not errors else 1

if __name__ == "__main__":
    sys.exit(main())
