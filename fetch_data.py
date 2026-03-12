"""
Fixed Income Terminal — Data Fetcher v2
=========================================
Sources (all free):
  US Treasury XML         → US yield curve        (no key)
  FRED API                → spreads, SOFR, history (free key: fred.stlouisfed.org/docs/api/api_key.html)
  ECB SDW REST API        → EU/Bund curves         (no key)
  Bank of Japan CSV       → JGB yields             (no key)
  FINRA Market Data API   → Agency + Corp volumes  (free key: developer.finra.org — needs client_id + client_secret)
  World Bank API          → China macro data       (no key)
  UK DMO XML              → Gilt yields            (no key)

Environment variables (set as GitHub Secrets):
  FRED_API_KEY          → FRED
  FINRA_CLIENT_ID       → FINRA OAuth2 client_id
  FINRA_CLIENT_SECRET   → FINRA OAuth2 client_secret

Run:  python fetch_data.py
Out:  data/bonds.json
"""

import json, os, sys, re, base64
from datetime import datetime, timezone, timedelta
import urllib.request, urllib.parse, urllib.error
import xml.etree.ElementTree as ET

OUTPUT_FILE        = "data/bonds.json"
FRED_KEY           = os.environ.get("FRED_API_KEY", "")
FINRA_CLIENT_ID    = os.environ.get("FINRA_CLIENT_ID", "")
FINRA_CLIENT_SECRET= os.environ.get("FINRA_CLIENT_SECRET", "")

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

def fetch_fred():
    if not FRED_KEY:
        raise RuntimeError("FRED_API_KEY not set")
    ig   = fetch_fred_series("BAMLC0A0CM")
    hy   = fetch_fred_series("BAMLH0A0HYM2")
    sofr = fetch_fred_series("SOFR")
    spr  = fetch_fred_series("T10Y2Y")
    # 24-month history for the spread chart
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
        "ig_spread_bps": ig["value"], "ig_date": ig["date"],
        "hy_spread_bps": hy["value"], "hy_date": hy["date"],
        "sofr":          sofr["value"], "sofr_date": sofr["date"],
        "t10y2y":        spr["value"], "t10y2y_date": spr["date"],
        "spread_history_labels": keys[-24:],
        "spread_history_us":     [monthly[k] for k in keys[-24:]],
    }


# ═══════════════════════════════════════════
# 3. ECB STATISTICAL DATA WAREHOUSE
#    data-api.ecb.europa.eu — no key
#
#  Correct key format:  B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3M
#  Maturity codes: SR_3M  SR_6M  SR_1Y  SR_2Y  SR_5Y  SR_10Y  SR_30Y
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
ECB_HEADERS = {
    "User-Agent": UA,
    "Accept":     "application/json",
}
# sdw-wsrest.ecb.europa.eu = older ECB SDMX API — more stable than data-api.ecb.europa.eu
ECB_BASE = "https://sdw-wsrest.ecb.europa.eu/service/data"

def fetch_ecb_yield(mat_code):
    key = f"B.U2.EUR.4F.G_N_A.SV_C_YM.{mat_code}"
    url = f"{ECB_BASE}/YC/{key}?lastNObservations=5&format=jsondata"
    data = fetch_json(url, headers=ECB_HEADERS)
    series = data["dataSets"][0]["series"]
    obs    = list(series.values())[0]["observations"]
    last   = obs[str(max(int(k) for k in obs))]
    return round(last[0], 4)

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
    # EURIBOR 3M
    euribor = None
    try:
        url = f"{ECB_BASE}/FM/B.U2.EUR.RT.MM.EURIBOR3MD_.HSTA?lastNObservations=1&format=jsondata"
        d   = fetch_json(url, headers=ECB_HEADERS)
        obs = list(d["dataSets"][0]["series"].values())[0]["observations"]
        euribor = round(list(obs.values())[-1][0], 4)
        print(f"    EURIBOR 3M: {euribor}%")
    except Exception as e:
        print(f"    EURIBOR skipped: {e}")
    if not tenors:
        raise RuntimeError("All ECB maturities failed — sdw-wsrest unreachable?")
    return {"tenors": tenors, "yields": yields, "euribor_3m": euribor}


# ═══════════════════════════════════════════
# 4. BANK OF JAPAN — JGB Yields
#    Japan Ministry of Finance CSV — no key
#  Stable URL: mof.go.jp publishes daily JGB closing yields
#  Columns: Date, 1Y, 2Y, 5Y, 10Y, 20Y, 30Y, 40Y
# ═══════════════════════════════════════════
def fetch_boj():
    # MoF publishes the complete history as a single CSV
    url = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/data/jgbcme_all.csv"
    csv_text = fetch_text(url, headers={
        "User-Agent": UA,
        "Accept":     "text/csv,*/*",
        "Referer":    "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/index.htm",
    })
    lines = [l.strip() for l in csv_text.splitlines() if l.strip()]

    # Find the header row (contains "Date" or "date" or year labels)
    header_idx = None
    for i, line in enumerate(lines):
        lower = line.lower()
        if ("date" in lower or "year" in lower) and "," in line:
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("MoF CSV: header row not found")

    headers = [h.strip().strip('"').lower() for h in lines[header_idx].split(",")]

    # Find last data row (starts with a date-like value)
    data_rows = [
        [c.strip().strip('"') for c in l.split(",")]
        for l in lines[header_idx + 1:]
        if l and re.match(r'\d{4}', l.strip())
    ]
    if not data_rows:
        raise RuntimeError("MoF CSV: no data rows found")
    last = data_rows[-1]

    # Map column name → standard tenor
    tenor_aliases = {
        "1y": "1Y", "1-year": "1Y", "1yr": "1Y",
        "2y": "2Y", "2-year": "2Y", "2yr": "2Y",
        "5y": "5Y", "5-year": "5Y", "5yr": "5Y",
        "10y": "10Y","10-year":"10Y","10yr":"10Y",
        "20y": "20Y","20-year":"20Y","20yr":"20Y",
        "30y": "30Y","30-year":"30Y","30yr":"30Y",
        "40y": "40Y","40-year":"40Y","40yr":"40Y",
    }
    tenors, yields = [], []
    for i, h in enumerate(headers[1:], start=1):
        norm = h.replace(" ", "").replace("_", "").replace("-year", "y")
        label = tenor_aliases.get(norm)
        if label and i < len(last):
            try:
                val = float(last[i])
                if 0 < val < 30:   # sanity check
                    tenors.append(label)
                    yields.append(round(val, 4))
            except ValueError:
                pass

    if not tenors:
        raise RuntimeError(f"MoF CSV: no tenors parsed. Headers: {headers[:8]}")

    return {"tenors": tenors, "yields": yields, "date": last[0]}


# ═══════════════════════════════════════════
# 5. FINRA TRACE API
#    api.finra.org — OAuth2 (free registration)
#    Register at developer.finra.org
#    You get client_id + client_secret — BOTH needed
#    GitHub Secrets: FINRA_CLIENT_ID  and  FINRA_CLIENT_SECRET
#
#  Note: if you only have one "API key" from FINRA, set it as
#  FINRA_CLIENT_ID and leave FINRA_CLIENT_SECRET empty —
#  the script will try a single-key auth as fallback.
# ═══════════════════════════════════════════
_finra_token = None

def get_finra_token():
    global _finra_token
    if _finra_token:
        return _finra_token
    if not FINRA_CLIENT_ID:
        raise RuntimeError("FINRA_CLIENT_ID not set in environment / GitHub Secrets")

    creds = base64.b64encode(
        f"{FINRA_CLIENT_ID}:{FINRA_CLIENT_SECRET}".encode()
    ).decode()

    # FINRA has changed their token endpoint URL over time — try both
    token_urls = [
        "https://ews.fip.finra.org/finra-api/oauth/client_credential/accesstoken",
        "https://api.finra.org/oauth2/client_credentials/accesstoken",
    ]
    last_err = None
    for token_url in token_urls:
        try:
            resp = fetch_json(
                token_url + "?grant_type=client_credentials",
                headers={
                    "Authorization":  f"Basic {creds}",
                    "Content-Type":   "application/x-www-form-urlencoded",
                    "Accept":         "application/json",
                },
                data=b"grant_type=client_credentials",
            )
            token = resp.get("access_token") or resp.get("token")
            if token:
                print(f"    FINRA token obtained via {token_url.split('/')[2]}")
                _finra_token = token
                return _finra_token
            last_err = f"No token in response keys: {list(resp.keys())}"
        except Exception as e:
            last_err = str(e)
            continue

    raise RuntimeError(
        f"FINRA token failed on all endpoints. Last error: {last_err}\n"
        "    → Check FINRA_CLIENT_ID and FINRA_CLIENT_SECRET at developer.finra.org"
    )

def finra_get(endpoint):
    token = get_finra_token()
    return fetch_json(
        f"https://api.finra.org/data/group/otcMarket/name/{endpoint}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept":        "application/json",
        },
    )

def parse_volume(v):
    try:
        return round(float(v) / 1e6, 1) if v else None
    except (TypeError, ValueError):
        return None

def fetch_finra():
    rows = finra_get("weeklySummary")
    result = {"date": None, "agency": {}, "corp_ig": {}, "corp_hy": {}, "agency_issuers": {}}

    for row in (rows if isinstance(rows, list) else []):
        if not result["date"]:
            result["date"] = row.get("reportDate") or row.get("weekEndingDate")
        market  = str(row.get("market", "")).lower()
        rtype   = str(row.get("reportType", row.get("productType", ""))).lower()
        entry = {
            "buy_volume_mm":      parse_volume(row.get("buyVolume")  or row.get("customerBuyVolume")),
            "sell_volume_mm":     parse_volume(row.get("sellVolume") or row.get("customerSellVolume")),
            "interdealer_vol_mm": parse_volume(row.get("interDealerVolume") or row.get("interDealerVol")),
            "trade_count":        row.get("tradeCount") or row.get("numberOfTrades"),
        }
        if "agency" in market:
            result["agency"] = entry
        elif "investment grade" in rtype or "ig" == rtype or "investment" in rtype:
            result["corp_ig"] = entry
        elif "high yield" in rtype or "hy" == rtype or "high" in rtype:
            result["corp_hy"] = entry

    # Agency issuer breakdown
    try:
        issuer_rows = finra_get("agencyIssuerSummary")
        for row in (issuer_rows if isinstance(issuer_rows, list) else []):
            name = row.get("issuerName", row.get("issuer", ""))
            vol  = row.get("totalVolume", row.get("volume"))
            if name:
                result["agency_issuers"][name] = parse_volume(vol)
    except Exception as e:
        print(f"    FINRA issuer breakdown skipped: {e}")

    if not result["agency"] and not result["corp_ig"]:
        raise RuntimeError("FINRA returned no usable data — check endpoint names")
    return result


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
    # Series: IRLTLT01.CNP.M = China long-term government bond yield (monthly)
    try:
        url  = "https://stats.oecd.org/SDMX-JSON/data/MEI_FIN/IRLTLT01.CNP.M/all?lastNObservations=3&format=jsondata"
        data = fetch_json(url, headers={"Accept": "application/json", "User-Agent": UA})
        series = data["dataSets"][0]["series"]
        obs    = list(series.values())[0]["observations"]
        last   = obs[str(max(int(k) for k in obs))]
        result["cgb_10y"] = round(last[0], 4)
        print(f"    OECD CGB 10Y: {result['cgb_10y']}%")
    except Exception as e:
        print(f"    OECD CGB 10Y: {e}")

    # ── FRED: China 10Y yield (if FRED key available) ───────────
    if FRED_KEY and "cgb_10y" not in result:
        try:
            url  = (f"https://api.stlouisfed.org/fred/series/observations"
                    f"?series_id=IRLTLT01CNM156N&api_key={FRED_KEY}&file_type=json"
                    f"&sort_order=desc&limit=3")
            data = fetch_json(url)
            for obs in data.get("observations", []):
                if obs.get("value") and obs["value"] != ".":
                    result["cgb_10y"]      = float(obs["value"])
                    result["cgb_10y_date"] = obs["date"]
                    print(f"    FRED CGB 10Y: {result['cgb_10y']}%")
                    break
        except Exception as e:
            print(f"    FRED CGB 10Y: {e}")

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
# 7. UK DMO — Gilt Yields
#    www.dmo.gov.uk XML — no key
# ═══════════════════════════════════════════
def fetch_uk_gilts():
    # UK DMO daily gilt closing yields
    url = "https://www.dmo.gov.uk/data/XmlDataReport?reportCode=D4A"
    text = fetch_text(url)
    root = ET.fromstring(text)

    # Find all data rows
    rows = (root.findall(".//ISDAGILTYIELD")
            or root.findall(".//{*}ISDAGILTYIELD")
            or root.findall(".//row")
            or root.findall(".//{*}row"))

    if not rows:
        # Try iterating all children of last entry
        all_items = list(root.iter())
        rows = [el for el in all_items if len(list(el)) > 3]

    if not rows:
        raise RuntimeError("UK DMO: no data rows found")

    last = rows[-1]
    tenors, yields, date_str = [], [], ""

    for child in last:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        tag_up = tag.upper()
        if not child.text or not child.text.strip():
            continue
        if "DATE" in tag_up or "ISIN" in tag_up:
            if "DATE" in tag_up:
                date_str = child.text.strip()
            continue
        try:
            val = float(child.text.strip())
            # Map tag to tenor
            for code, label in [("2Y","2Y"),("5Y","5Y"),("10Y","10Y"),("15Y","15Y"),("20Y","20Y"),("25Y","25Y"),("30Y","30Y"),("40Y","40Y"),("50Y","50Y"),
                                 ("2","2Y"),("5","5Y"),("10","10Y"),("20","20Y"),("30","30Y")]:
                if tag_up.endswith(code.replace("Y","")) or code.replace("Y","") in tag_up:
                    if label not in tenors:
                        tenors.append(label)
                        yields.append(round(val, 4))
                    break
        except ValueError:
            pass

    # If tag matching failed, just take numeric columns in order
    if not tenors:
        standard = ["2Y","5Y","10Y","15Y","20Y","30Y"]
        numeric_vals = []
        for child in last:
            try:
                v = float(child.text.strip())
                if 0 < v < 20:
                    numeric_vals.append(round(v, 4))
            except (ValueError, AttributeError):
                pass
        tenors = standard[:len(numeric_vals)]
        yields = numeric_vals[:len(standard)]

    if not tenors:
        raise RuntimeError("UK DMO: could not parse any yield values")

    return {"tenors": tenors, "yields": yields, "date": date_str}


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════
def main():
    print("\n🔄 Fixed Income Terminal — Data Fetcher v2")
    print(f"   Timestamp:    {datetime.now(timezone.utc).isoformat()}")
    print(f"   FRED key:     {'✅ set' if FRED_KEY else '❌ missing — set FRED_API_KEY'}")
    print(f"   FINRA id:     {'✅ set' if FINRA_CLIENT_ID else '❌ missing — set FINRA_CLIENT_ID'}")
    print(f"   FINRA secret: {'✅ set' if FINRA_CLIENT_SECRET else '❌ missing — set FINRA_CLIENT_SECRET'}\n")

    output = {"last_updated": datetime.now(timezone.utc).isoformat(), "errors": {}}

    steps = [
        ("US Treasury yield curve",      fetch_us_treasury,  "us_treasury"),
        ("FRED spreads + history",        fetch_fred,         "fred"),
        ("ECB yield curves",             fetch_ecb,          "ecb"),
        ("Bank of Japan JGB",            fetch_boj,          "boj"),
        ("FINRA TRACE volumes",          fetch_finra,        "finra"),
        ("China bond market",            fetch_china_bonds,  "china"),
        ("UK Gilt yields",               fetch_uk_gilts,     "uk_gilts"),
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
