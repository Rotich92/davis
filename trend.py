"""
material_news_monitor.py
On-demand global news watcher for Movit Products Limited (MPL).

- Sources: Google News RSS + GDELT (no API keys)
- Scope: Global supply chain + East/Southern Africa downstream impacts
- Output: ./out/news_YYYY-MM-DD.xlsx and .csv (Excel-safe, tz-naive datetimes)
- Optional: Slack/MS Teams alerts (set webhooks)

Install:
  pip install feedparser pandas python-dateutil requests beautifulsoup4 rapidfuzz openpyxl

Run:
  python material_news_monitor.py
"""

from __future__ import annotations
import os, re, json, time, hashlib, html, random
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus
import requests
import feedparser
import pandas as pd
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from dateutil import tz

# =============== CONFIG (MPL-aware) ===============
LOCAL_TZ = tz.gettz("Africa/Kampala")  # MPL HQ timezone

# Optional alerts (leave "" to disable)
SLACK_WEBHOOK = ""     # e.g. "https://hooks.slack.com/services/XXX/YYY/ZZZ"
TEAMS_WEBHOOK = ""     # e.g. "https://outlook.office.com/webhook/..."

MAX_ITEMS_PER_MATERIAL_PER_SOURCE = 12
MIN_RELEVANCE = 60         # tune to reduce/expand noise
GDELT_DAYS_BACK = 10       # wider window to catch upstream events
HTTP_TIMEOUT = 20
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = (1.2, 2.0)

# ===== Pinned, must-watch external signals (always included) =====
PINNED_SIGNALS = [
    "https://www.bbc.com/news/articles/c9qy98gp474o",  # your key BBC insight
    # add more URLs here as needed
]

# MPL context signals (boost if mentioned)
MPL_TERMS = [
    "Movit", "Movit Products", "MPL", "Radiant", "Movit Uganda", "Movit East Africa",
    "cosmetics", "personal care", "hair care", "skin care", "beauty industry"
]

# Downstream demand/ops regions (where MPL sells/distributes)
EAST_AF_HINTS = [
    "Uganda", "Kenya", "Tanzania", "Rwanda", "Burundi", "DRC", "Zambia",
    "South Sudan", "Malawi", "Mozambique", "Angola", "Zimbabwe", "Nigeria",
    "East Africa", "COMESA", "EAC"
]

# Upstream sourcing/logistics/commodity signals (global)
GLOBAL_SUPPLY_HINTS = [
    # Logistics
    "Suez Canal", "Panama Canal", "Strait of Hormuz", "Red Sea", "Cape of Good Hope",
    "port congestion", "port strike", "rail strike", "truckers strike", "freight rates",
    "container shortage", "IMO 2020", "shipping disruption", "supply chain disruption",
    "Houthi", "piracy", "Maersk", "MSC", "CMA CGM", "Evergreen", "Hapag-Lloyd",
    # Macro commodities
    "Brent", "WTI", "naphtha", "paraffin", "propane", "butane", "natural gas",
    "feedstock", "oleochemicals", "palm oil", "palm kernel oil", "PKO", "coconut oil",
    "tallow", "ethanol", "isopropanol", "propylene", "ethylene", "benzene",
    # Regulatory
    "export ban", "export curbs", "sanctions", "REACH", "IFRA", "EU cosmetics regulation",
    "US FDA cosmetics", "Kenya Bureau of Standards", "UNBS", "tax", "levy", "tariff", "VAT"
]

# Your materials (as provided)
MATERIALS = [
    "LPG GAS", "SOLVENT BLUE", "POLYSORBATE 20",
    "AFTER SHAVE BALM 250ML CONTAINER",
    "RESIZED MOVIT BRANDED WOOVEN BAGS",
    "EXTRA NEUTRAL SPIRIT (PREMIUM)", "KANZIRONZIRO",
    "RADIANT NEUTRALISING SHAMPOO 500ML-BOXES", "PHOSPHERIC ACID",
    "CARBOPOL 990", "EUPERLAN PK 771BENZ (UFABLEND MPL)",
    "ANTI HAIR LOSS SHAMPOO", "ARGAN OIL HAIR MASK RM", "BLENDING WAX",
    "WHITE MIRACLE -862681", "SILKY CREAM- 713651",
    "AVOCADO /SHEA BUTTER FRAGRANCE -1022276", "ORCHID PERFUME (1022222)",
    "CETOSTEARYL ALCOHOL (LANETTE  D )", "HAIR OIL-RM", "OLIVE TREATMENT",
    "COCOA BUTTER GLOW BODY OIL", "EGG TREATMENT", "TINOGARD TL",
    "GLYCERINE  OIL", "AEROSOL PROPELLANT GAS", "RELAXING SHEEN",
    "WHITE OIL/ LIQUID PARAFFIN LIGHT", "ACETONE", "VISCUP 160",
    "SALIETHANOL", "ELEMENT-701358", "CRODOMOL STS", "KERAVIS",
    "CETEARETH 25", "SODIUM BENZOATE POWDER BP 2017", "OLIVE CARE E_1518341",
    "PVP K 30", "BEHENTRIMONIUM CHLORIDE", "PARAPHENYL DIAMINE",
    "METHYL PARABEN B.P", "CETIOL AB", "DEHYQUAT  F 75",
    "CREMOPHOR A6/EMULGADE A6", "LUVIQUAT PQ 11AT 1",
    "PLANTACARE 818 UP", "LEMON FRAGRANCE", "ORANGE GEL (250125)",
    "NOIR VERT 215372", "ZANAHEM PERFUME (201009)", "EFF 261049 SHIELDED",
    "COLANYL YELLOW G 132-ZA", "TRIETHANOLAMINE",
    "SLES 70%(TEXAPON) SODIUM LAURYL ETHER SU", "RT79948 LEMON SOAP",
    "JINTINT YELLOW 3930 EP", "HYDROGEN PEROXIDE 50%",
    "THIOGLYCOLIC ACID 99%", "REFINED SULPHUR POWDER",
    "XIAMETER (PMX 245)/SK.SF 1202"
]

# Synonyms / expansions (improve recall for trade names, chem aliases)
SYNONYMS = {
    "LPG GAS": ["liquefied petroleum gas", "propane", "butane", "autogas"],
    "SLES 70%(TEXAPON) SODIUM LAURYL ETHER SU": ["SLES 70", "sodium laureth sulfate", "texapon", "lauryl ether sulfate"],
    "PHOSPHERIC ACID": ["phosphoric acid", "H3PO4"],
    "CETOSTEARYL ALCOHOL (LANETTE  D )": ["cetearyl alcohol", "cetostearyl alcohol", "lanette d"],
    "TRIETHANOLAMINE": ["TEA", "triethanolamine"],
    "ACETONE": ["propanone"],
    "HYDROGEN PEROXIDE 50%": ["H2O2 50%", "hydrogen peroxide solution"],
    "THIOGLYCOLIC ACID 99%": ["mercaptoacetic acid", "thioglycolic acid"],
    "REFINED SULPHUR POWDER": ["sulfur powder", "sulphur powder"],
    "AEROSOL PROPELLANT GAS": ["propellant gas", "aerosol propellant", "LPG propellant"],
    "WHITE OIL/ LIQUID PARAFFIN LIGHT": ["white mineral oil", "liquid paraffin", "white mineral oil light"],
    "CARBOPOL 990": ["carbomer 990", "carbopol polymer"],
    "CETEARETH 25": ["ceteareth-25"],
    "BEHENTRIMONIUM CHLORIDE": ["BTAC", "behentrimonium chloride"],
    "SODIUM BENZOATE POWDER BP 2017": ["sodium benzoate"],
    "PVP K 30": ["polyvinylpyrrolidone k30", "povidone k30"],
    "GLYCERINE  OIL": ["glycerin", "glycerol"],
    "POLYSORBATE 20": ["tween 20", "polyoxyethylene (20) sorbitan monolaurate"],
    "CRODOMOL STS": ["cetearyl ethylhexanoate (Crodamol STS)", "crodamol sts"],
    "PLANTACARE 818 UP": ["caprylyl/capryl glucoside", "plantacare 818"],
    "XIAMETER (PMX 245)/SK.SF 1202": ["octamethyltrisiloxane", "OMTS", "PMX-245", "silicone solvent"]
}

# Regions/ports to scan for global disruption context
GLOBAL_PORTS = [
    "Shanghai", "Ningbo", "Shenzhen", "Tianjin", "Qingdao", "Busan",
    "Singapore", "Port Klang", "Tanjung Pelepas", "Nhava Sheva", "Mundra",
    "Jebel Ali", "Rotterdam", "Antwerp", "Hamburg", "Felixstowe",
    "Durban", "Mombasa", "Dar es Salaam", "Djibouti", "Walvis Bay"
]

HEADERS = {"User-Agent": "Mozilla/5.0 (MaterialNewsBot/1.1; +https://mpl.local)"}

# =============== UTILS ===============
def today_local() -> datetime:
    return datetime.now(tz=LOCAL_TZ)

def clean_text(x: str) -> str:
    if not x:
        return ""
    x = html.unescape(x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def summarize_text(html_or_text: str, max_chars: int = 280) -> str:
    text = BeautifulSoup(html_or_text or "", "html.parser").get_text(" ")
    text = clean_text(text)
    if len(text) <= max_chars:
        return text
    parts = re.split(r"(?<=[.!?])\s+", text)
    out = ""
    for p in parts:
        if not p:
            continue
        if len(out) + len(p) + 1 > max_chars:
            break
        out += (p + " ")
    return out.strip() or (text[:max_chars] + "…")

def hash_id(*parts) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
    return h.hexdigest()[:16]

def expand_queries(material: str) -> list[str]:
    qs = [material]
    for syn in SYNONYMS.get(material, []):
        qs.append(syn)
    return list(dict.fromkeys(qs))

def backoff_sleep(attempt: int):
    lo, hi = RETRY_BACKOFF
    time.sleep((lo + (hi - lo) * random.random()) * attempt)

# =============== FETCHERS ===============
def safe_get(url: str, params: dict | None = None) -> requests.Response | None:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return r
        except Exception:
            pass
        if attempt < RETRY_ATTEMPTS:
            backoff_sleep(attempt)
    return None

def fetch_google_news(material: str) -> list[dict]:
    out = []
    # Query combines material/synonyms plus global context keywords to widen recall
    base_variants = expand_queries(material)
    context_terms = ["supply", "shortage", "price", "export", "imports", "logistics", "shipment",
                     "factory", "plant", "shutdown", "strike", "regulation", "tariff", "recall"]
    for q in base_variants:
        q_full = f'"{q}" ("' + '" OR "'.join(context_terms) + '")'
        rss_url = f"https://news.google.com/rss/search?q={quote_plus(q_full)}&hl=en-GB&gl=GB&ceid=GB:en"
        feed = feedparser.parse(rss_url)
        for e in feed.entries[:MAX_ITEMS_PER_MATERIAL_PER_SOURCE]:
            title = clean_text(e.get("title", ""))
            link = e.get("link", "")
            link = re.sub(r"^https?://news\.google\.com/.*url=([^&]+).*",
                          lambda m: requests.utils.unquote(m.group(1)), link)
            published = e.get("published", "") or e.get("updated", "")
            summary = summarize_text(e.get("summary", ""))
            out.append({
                "material": material,
                "query_variant": q,
                "title": title,
                "url": link,
                "published": published,
                "source": "GoogleNewsRSS",
                "summary": summary
            })
    return out

def fetch_gdelt(material: str) -> list[dict]:
    out = []
    start = (today_local() - timedelta(days=GDELT_DAYS_BACK)).strftime("%Y%m%d%H%M%S")
    end   = today_local().strftime("%Y%m%d%H%M%S")
    gdelt_url = "https://api.gdeltproject.org/api/v2/doc/doc"
    base_variants = expand_queries(material)
    # Add a couple of global context queries per material
    extra_queries = [
        f'{base_variants[0]} supply OR shortage OR export OR import',
        f'{base_variants[0]} price OR tariff OR regulation OR logistics'
    ]
    for q in list(dict.fromkeys(base_variants + extra_queries)):
        params = {
            "query": q,
            "mode": "ArtList",
            "maxrecords": "60",
            "format": "json",
            "startdatetime": start,
            "enddatetime": end
        }
        r = safe_get(gdelt_url, params=params)
        if not r:
            continue
        try:
            data = r.json()
        except Exception:
            continue
        for a in data.get("articles", [])[:MAX_ITEMS_PER_MATERIAL_PER_SOURCE]:
            title = clean_text(a.get("title"))
            url = a.get("url")
            ts = a.get("seendate") or a.get("publishedDate")
            desc = clean_text(a.get("seendescription") or a.get("sourceCommonName") or "")
            out.append({
                "material": material,
                "query_variant": q,
                "title": title,
                "url": url,
                "published": ts,
                "source": "GDELT",
                "summary": desc
            })
    return out

# =============== SCORING / FILTERING (MPL + global) ===============
def relevance_score(row: dict) -> int:
    text = f"{row.get('title','')} {row.get('summary','')}"
    text_l = text.lower()

    # 1) Match the material & synonyms
    base = 0
    for t in expand_queries(row["material"]):
        base = max(base, fuzz.token_set_ratio(t.lower(), text_l))

    # 2) Boost if MPL or personal-care context appears
    mpl_boost = 0
    for t in MPL_TERMS:
        mpl_boost = max(mpl_boost, 0.6 * fuzz.partial_ratio(t.lower(), text_l))

    # 3) Boost for East/Southern Africa downstream markets
    east_boost = 0
    for rgn in EAST_AF_HINTS:
        east_boost = max(east_boost, 0.35 * fuzz.partial_ratio(rgn.lower(), text_l))

    # 4) Boost for global supply/logistics/commodity hints
    global_boost = 0
    for kw in GLOBAL_SUPPLY_HINTS + GLOBAL_PORTS:
        global_boost = max(global_boost, 0.35 * fuzz.partial_ratio(kw.lower(), text_l))

    # 5) Penalise generic/irrelevant topics
    generic = any(k in text_l for k in ["celebrity", "football", "movie", "lottery", "gossip"])
    penalty = 12 if generic else 0

    score = int(min(100, base + mpl_boost + east_boost + global_boost - penalty))
    return score

def deduplicate(rows: list[dict]) -> list[dict]:
    seen = set()
    keep = []
    for r in rows:
        hid = hash_id(r.get("url",""), r.get("title",""))
        if hid in seen:
            continue
        dup = False
        for u in keep:
            if fuzz.token_set_ratio((r.get("title","")).lower(), (u["title"]).lower()) >= 92:
                dup = True
                break
        if not dup:
            seen.add(hid)
            keep.append(r)
    return keep

def normalize_date(s: str):
    """
    Parse common feed timestamps, convert to Africa/Kampala, and return
    a timezone-NAIVE datetime (Excel-safe).
    """
    if not s:
        return None
    fmts = [
        "%a, %d %b %Y %H:%M:%S %Z",  # RSS
        "%Y%m%d%H%M%S",              # GDELT seendate
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt.astimezone(LOCAL_TZ).replace(tzinfo=None)  # <-- strip tz
        except Exception:
            continue
    return None

# =============== PINNED SIGNALS SUPPORT ===============
def fetch_page_meta(url: str) -> tuple[str, str]:
    """Try to fetch <title> and a short description for a pinned URL."""
    try:
        r = safe_get(url)
        if not r:
            return ("Pinned: External insight", "")
        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.get_text(strip=True) if soup.title else "Pinned: External insight"
        desc = ""
        for name in ["description", "og:description", "twitter:description"]:
            tag = soup.find("meta", attrs={"name": name}) or soup.find("meta", attrs={"property": name})
            if tag and tag.get("content"):
                desc = tag["content"].strip()
                break
        return (clean_text(title), clean_text(desc))
    except Exception:
        return ("Pinned: External insight", "")

# =============== ALERTS ===============
def post_webhook(webhook_url: str, text: str) -> None:
    if not webhook_url:
        return
    try:
        requests.post(webhook_url, json={"text": text}, headers=HEADERS, timeout=HTTP_TIMEOUT)
    except Exception:
        pass

def format_alert(df: pd.DataFrame, limit: int = 10) -> str:
    lines = ["*MPL Material News — Top Global Signals*"]
    sample = df.sort_values(["Relevance","Published"], ascending=[False, False]).head(limit)
    for _, r in sample.iterrows():
        date_str = r["Published"].strftime("%Y-%m-%d %H:%M") if pd.notnull(r["Published"]) else ""
        lines.append(f"• *{r['Material']}* — {r['Title']}  ({date_str})\n  {r['URL']}")
    return "\n".join(lines)

# =============== MAIN ===============
def main():
    print(f"[{today_local():%Y-%m-%d %H:%M}] MPL global material news scan…")
    all_rows: list[dict] = []

    # Pull dynamic items per material
    for mat in MATERIALS:
        try:
            rss_rows = fetch_google_news(mat)
            gd_rows  = fetch_gdelt(mat)
            rows = rss_rows + gd_rows
            for r in rows:
                r["score"] = relevance_score(r)
            rows = [r for r in rows if r["score"] >= MIN_RELEVANCE]
            all_rows.extend(rows)
        except Exception as ex:
            print(f"[WARN] Failure for '{mat}': {ex}")

    # Include pinned insights (always-on)
    pinned_rows = []
    for u in PINNED_SIGNALS:
        title, desc = fetch_page_meta(u)
        pinned_rows.append({
            "material": "MULTI / GLOBAL",
            "query_variant": "Pinned",
            "title": title if title else "Pinned: External insight",
            "url": u,
            "published": today_local().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"),
            "source": "Pinned",
            "summary": desc,
            "score": 100  # force to top tier
        })
    all_rows.extend(pinned_rows)

    # Dedupe
    all_rows = deduplicate(all_rows)

    if not all_rows:
        print("[INFO] No items above threshold.")
        return

    # Build DataFrame (ensure tz-naive for Excel)
    recs = []
    for r in all_rows:
        pub_dt = normalize_date(r.get("published","")) or today_local().replace(tzinfo=None)
        recs.append({
            "Material": r["material"],
            "QueryVariant": r.get("query_variant",""),
            "Source": r["source"],
            "Title": r["title"],
            "Summary": r.get("summary",""),
            "URL": r.get("url",""),
            "Published": pub_dt,   # tz-naive
            "Relevance": r["score"]
        })
    df = pd.DataFrame(recs)
    df.sort_values(["Material","Relevance","Published"], ascending=[True, False, False], inplace=True)

    # Output files
    os.makedirs("out", exist_ok=True)
    stamp = today_local().strftime("%Y-%m-%d")
    xlsx_path = f"out/news_{stamp}.xlsx"
    csv_path  = f"out/news_{stamp}.csv"

    # Save (Excel-safe)
    df.to_excel(xlsx_path, index=False)
    df.to_csv(csv_path, index=False, encoding="utf-8")

    print(f"[OK] Saved {len(df)} items -> {xlsx_path} and {csv_path}")

    # Optional alerts
    if SLACK_WEBHOOK:
        post_webhook(SLACK_WEBHOOK, format_alert(df))
    if TEAMS_WEBHOOK:
        post_webhook(TEAMS_WEBHOOK, format_alert(df))

    print("[DONE]")

if __name__ == "__main__":
    main()

#thanks you very much