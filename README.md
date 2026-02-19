
# CMS MA SCC — Pure PowerShell (Rolling 24m, 48 States, Parent Org) — **Patched**

This repo builds a **rolling 24 months** dataset of **Monthly Medicare Advantage (MA) Enrollment by State/County/Contract (SCC)**, always downloading the **Full version** per month and enriching with **Parent Organization** from the **MA Plan Directory**. It also includes a static, shareable dashboard under `/dashboard/` that reads the CSVs from `data/processed/`.

> **What’s patched?**
> Some CMS monthly pages use a Dynamic List widget that injects download links client‑side. The original scraper could miss those links. This version adds a robust file link discovery that scans for `/files/...` URLs in both anchors and embedded JSON and sets a modern User‑Agent so CMS returns complete markup.

## Quick start

```powershell
# 1) Run ETL once to generate CSVs
.\Invoke-CmsMaSccRefresh.ps1 -RollingMonths 24

# 2) Commit & push to GitHub so the dashboard can read the files
git add data/processed/*.csv
git commit -m "Add processed CSVs"
git push

# 3) Enable GitHub Pages → Settings → Pages → Source: main, Folder: /(root)
#    Open: https://<your-username>.github.io/<repo-name>/dashboard/
```

## Files
- `Invoke-CmsMaSccRefresh.ps1` — ETL (SCC Full, Parent Org, 48 states, rolling 24m)
- `.github/workflows/refresh.yml` — monthly automation (16th @13:15 UTC) + manual trigger
- `dashboard/` — Plotly.js static dashboard
- `data/processed/` — outputs (CSV) will appear here after first run

## Notes
- If a month is published only as Excel without CSV/ZIP, the script skips that month (to remain dependency‑free). You’ll see a warning in logs.
- CMS indicates monthly enrollment files are generally posted by the **15th**; the workflow runs on the **16th** to avoid racing publication.
