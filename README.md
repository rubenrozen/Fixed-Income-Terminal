# Fixed Income Terminal

Global bond market dashboard — live data, zero simulated numbers.

## 🚀 Setup (5 minutes)

### 1. Get your free API keys

| Key | Where | Time |
|-----|-------|------|
| `FRED_API_KEY` | [fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html) | 30 sec |
| `FINRA_CLIENT_ID` + `FINRA_CLIENT_SECRET` | [developer.finra.org](https://developer.finra.org) — Register, create an app, copy both values | 2 min |

### 2. Add keys as GitHub Secrets

```
GitHub repo → Settings → Secrets and variables → Actions → New repository secret
```
Add three secrets: `FRED_API_KEY`, `FINRA_CLIENT_ID`, `FINRA_CLIENT_SECRET`

### 3. Run the workflow once manually

```
GitHub → Actions → "Update Bond Market Data" → Run workflow
```
This creates `data/bonds.json`. After that, it runs **every Monday at 06:00 UTC** automatically.

### 4. Enable GitHub Pages

```
GitHub → Settings → Pages → Deploy from branch → main / (root)
```
Your dashboard: `https://rubenrozen.github.io/Fixed-Income-Terminal/`

---

## 📊 Data Sources

| Section | Source | Key needed | Frequency |
|---------|--------|-----------|-----------|
| US yield curve (3M→30Y) | US Treasury XML | ❌ None | Daily (fetched weekly) |
| IG/HY Credit Spreads | FRED BAMLC0A0CM / BAMLH0A0HYM2 | ✅ FRED | Daily |
| SOFR | FRED | ✅ FRED | Daily |
| 2Y-10Y Spread history | FRED T10Y2Y | ✅ FRED | Daily |
| EU yield curve | ECB Statistical Data Warehouse | ❌ None | Daily |
| EURIBOR 3M | ECB SDW | ❌ None | Daily |
| JGB yields | Bank of Japan (IR01) | ❌ None | Daily |
| UK Gilt yields | UK DMO XML | ❌ None | Daily |
| Agency bond volumes (FNMA/FHLB/FHLMC) | FINRA TRACE API | ✅ FINRA | Weekly |
| Corp 144A volumes (IG/HY) | FINRA TRACE API | ✅ FINRA | Weekly |
| China bond market | ADB Asian Bonds Online + CCDC + World Bank | ❌ None | Weekly |

### ⚠️ Honest limitations

- **FINRA TRACE**: volumes available — detailed Affiliate/Customer/Inter-dealer breakdown depends on which endpoints FINRA exposes to your API tier. Free tier may return aggregate only.
- **China bonds**: ADB and CCDC data is available but granularity varies. CCDC may block GitHub Actions IP ranges — if so, the World Bank indicator still loads.
- **Derivatives heatmap / Swaption vol surface**: no free real-time futures or vol data exists. The structure display is illustrative. For real data: CME DataMine (paid) or Interactive Brokers API.

---

## 🔄 Manual data refresh

```bash
# Install nothing — pure stdlib Python 3.11+
FRED_API_KEY=your_key FINRA_API_KEY=your_key python fetch_data.py
```

Output: `data/bonds.json` — commit and push to update the live site immediately.

---

## 📁 File structure

```
Fixed-Income-Terminal/
├── index.html              ← Dashboard (GitHub Pages serves this)
├── fetch_data.py           ← Data fetcher (Python stdlib only)
├── data/
│   └── bonds.json          ← Auto-updated by GitHub Actions
└── .github/
    └── workflows/
        └── update_data.yml ← Weekly cron job
```
