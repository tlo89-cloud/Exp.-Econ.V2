"""
Experience Economy Map — Daily News Scraper
Runs at 7am EST (12:00 UTC) via GitHub Actions.
Fetches from ~15 sources, scores articles for relevance,
maps to layers, writes top results to events.json.
"""

import feedparser
import json
import os
import re
import hashlib
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError
import ssl

# ── CONFIGURATION ─────────────────────────────────────────────────────

OUTPUT_FILE = "events.json"
MAX_EVENTS = 60          # total events kept in JSON
MAX_PER_LAYER = 8        # max new events per layer per run
MIN_SCORE = 3            # minimum relevance score to include

# ── NEWS SOURCES ──────────────────────────────────────────────────────

SOURCES = [
    # Tier 1 — Sports & Entertainment Business
    {"url": "https://www.sportico.com/feed/",                   "name": "Sportico"},
    {"url": "https://www.sportsbusinessjournal.com/rss.aspx",   "name": "SBJ"},
    {"url": "https://frontofficesports.com/feed/",              "name": "Front Office Sports"},
    {"url": "https://www.billboard.com/feed/",                  "name": "Billboard"},
    {"url": "https://variety.com/v/music/feed/",                "name": "Variety Music"},
    {"url": "https://variety.com/v/biz/feed/",                  "name": "Variety Biz"},
    {"url": "https://pollstar.com/rss/news",                    "name": "Pollstar"},
    {"url": "https://www.musicweek.com/rss",                    "name": "Music Week"},

    # Tier 2 — Finance / Deals / PE
    {"url": "https://pitchbook.com/news/rss",                   "name": "PitchBook"},
    {"url": "https://www.pehub.com/feed/",                      "name": "PE Hub"},
    {"url": "https://mergersandinquisitions.com/feed/",         "name": "M&I"},
    {"url": "https://www.wsj.com/xml/rss/3_7085.xml",           "name": "WSJ Sports"},

    # Tier 3 — Venue & Infrastructure
    {"url": "https://www.venuestoday.com/rss",                  "name": "Venues Today"},
    {"url": "https://stadiumsarenas.com/feed/",                 "name": "Stadiums & Arenas"},
    {"url": "https://www.thestadiumguide.com/feed/",            "name": "Stadium Guide"},

    # Tier 4 — Tech & Data
    {"url": "https://www.sporttechie.com/feed/",                "name": "Sport Techie"},
    {"url": "https://www.theverge.com/rss/index.xml",           "name": "The Verge"},  # filtered heavily

    # Tier 5 — Ticketing & Distribution
    {"url": "https://www.ticketnews.com/feed/",                 "name": "Ticket News"},
    {"url": "https://www.broadwayworld.com/rss/news.cfm",       "name": "Broadway World"},
]

# ── KEYWORD SCORING ───────────────────────────────────────────────────

# High-value business signals (+3 each)
HIGH_VALUE = [
    "acquisition", "acquires", "acquired", "merger", "merges", "merged",
    "valuation", "valued at", "funding", "fundraise", "raises", "raised",
    "series a", "series b", "series c", "ipo", "goes public", "listing",
    "private equity", "pe firm", "investment", "invested", "investor",
    "media rights", "rights deal", "broadcasting deal", "tv deal",
    "stadium", "arena", "venue", "concert", "festival", "tour",
    "franchise", "ownership", "stake", "billion", "million",
    "expansion", "launched", "launches", "partnership", "deal signed",
    "antitrust", "doj", "lawsuit", "ruling",
]

# Layer-specific keywords (+2 each, used for layer mapping too)
LAYER_KEYWORDS = {
    1: [  # Content & IP
        "nfl", "nba", "mlb", "nhl", "mls", "premier league", "formula one", "f1",
        "ufc", "wwe", "tko", "wnba", "nwsl", "sports franchise", "league",
        "music rights", "touring", "artist", "concert", "festival", "coachella",
        "spotify", "universal music", "warner music", "record label",
        "sports ip", "media rights", "broadcast rights", "streaming rights",
        "arctos", "redbird", "sixth street", "ares management", "liberty media",
        "comedy", "theater", "broadway", "immersive",
    ],
    2: [  # Physical Infrastructure
        "stadium", "arena", "venue", "sphere entertainment", "cosm",
        "topgolf", "puttshack", "competitive socializing", "theme park",
        "disney parks", "universal studios", "six flags", "oak view group",
        "legends global", "aeg", "msg entertainment", "intuit dome",
        "mixed-use", "entertainment district", "real estate", "development",
        "rebuild", "renovation", "new arena", "new stadium",
        "meow wolf", "immersive venue",
    ],
    3: [  # Operators & Distribution
        "live nation", "ticketmaster", "aeg presents", "ticketing",
        "stubhub", "seatgeek", "viagogo", "secondary market",
        "netflix sports", "amazon prime sports", "apple tv sports", "espn",
        "dazn", "streaming deal", "broadcast", "discovery",
        "fever", "eventbrite", "bandsintown",
        "doj live nation", "antitrust", "ticket fees",
        "promoter", "concert promotion", "tour",
    ],
    4: [  # Service Providers
        "caa", "wme", "uta", "talent agency", "representation",
        "learfield", "playfly", "college sports rights",
        "on location", "hospitality", "premium seating", "vip",
        "aramark", "sodexo", "delaware north", "food and beverage",
        "tait", "nep group", "prg", "production", "staging",
        "sponsorship", "two circles", "sportfive", "nielsen sports",
        "wasserman", "octagon", "img",
    ],
    5: [  # Technology Enablers
        "sportradar", "genius sports", "hawk-eye", "betting data",
        "sports betting", "wagering", "sportsbook", "kambi",
        "fan engagement", "fan app", "venue technology", "smart stadium",
        "tickpick", "wicket", "biometric", "facial recognition",
        "greenfly", "content technology", "broadcast technology",
        "fanatics", "merchandise", "e-commerce", "nft",
        "sports analytics", "data platform", "ai sports",
        "appetize", "pos", "mobile ordering", "venue software",
    ],
}

# Noise filters — auto-exclude if these appear in title (-10 score)
NOISE_KEYWORDS = [
    "injury report", "injury update", "day-to-day", "out for season",
    "game recap", "match report", "final score", "highlights",
    "fantasy football", "fantasy baseball", "fantasy points",
    "betting odds", "spread", "over/under", "prop bet",
    "roster move", "trade deadline", "waiver wire", "depth chart",
    "mock draft", "nfl draft picks", "prospect ranking",
    "power rankings", "standings", "schedule release",
    "watch live", "how to watch", "stream free",
]

# Access type hints from article content
ACCESS_HINTS = {
    "public":  ["nyse", "nasdaq", "stock", "shares", "public company", "publicly traded", "earnings", "quarterly"],
    "pe":      ["private equity", "pe firm", "buyout", "acquisition", "family office", "institutional"],
    "venture": ["venture", "series a", "series b", "series c", "seed round", "startup", "vc backed"],
}

# ── SCORING ENGINE ────────────────────────────────────────────────────

def score_article(title, summary=""):
    """Score an article for relevance. Returns (score, layer, access)."""
    text = (title + " " + summary).lower()

    # Instant disqualify — noise
    for noise in NOISE_KEYWORDS:
        if noise in text:
            return -10, 0, "ref"

    score = 0

    # High-value business signal keywords
    for kw in HIGH_VALUE:
        if kw in text:
            score += 3

    # Layer matching
    layer_scores = {}
    for layer, keywords in LAYER_KEYWORDS.items():
        layer_score = sum(2 for kw in keywords if kw in text)
        if layer_score > 0:
            layer_scores[layer] = layer_score
        score += layer_score

    # Pick the highest-scoring layer
    best_layer = max(layer_scores, key=layer_scores.get) if layer_scores else 0

    # Access type detection
    access = "pe"  # default
    for atype, hints in ACCESS_HINTS.items():
        if any(h in text for h in hints):
            access = atype
            break

    return score, best_layer, access


def clean_text(text, max_len=120):
    """Strip HTML and truncate."""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Truncate
    if len(text) > max_len:
        text = text[:max_len].rsplit(' ', 1)[0] + '...'
    return text


def extract_company(title):
    """Try to extract a company/entity name from the article title."""
    # Look for patterns like "Company Name: ..." or "Company Name raises..."
    patterns = [
        r'^([A-Z][A-Za-z0-9\s&]+?(?:Entertainment|Sports|Group|Media|Music|Live|Nation|Partners|Capital))\b',
        r'^([A-Z][A-Za-z0-9]+(?:\s[A-Z][A-Za-z0-9]+)?)\s+(?:raises|acquires|launches|signs|announces|closes)',
        r'^([A-Z][A-Za-z0-9\s]+?):',
    ]
    for pattern in patterns:
        match = re.match(pattern, title)
        if match:
            company = match.group(1).strip()
            if 2 < len(company) < 40:
                return company
    return ""


def format_date(entry):
    """Extract and format article date."""
    try:
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.strftime("%b %Y")
    except Exception:
        pass
    return datetime.now(timezone.utc).strftime("%b %Y")


def article_id(title, url):
    """Stable ID for deduplication."""
    key = (title + url).encode('utf-8')
    return hashlib.md5(key).hexdigest()[:12]

# ── FETCHING ──────────────────────────────────────────────────────────

def fetch_feed(source):
    """Fetch and parse an RSS feed. Returns list of articles."""
    articles = []
    try:
        # Use SSL context that's more permissive for older feeds
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        req = Request(
            source["url"],
            headers={"User-Agent": "Mozilla/5.0 (compatible; ExperienceEconBot/1.0)"}
        )
        raw = urlopen(req, context=ctx, timeout=15).read()
        feed = feedparser.parse(raw)

        for entry in feed.entries[:30]:  # max 30 per source
            title = clean_text(getattr(entry, 'title', ''), 150)
            summary = clean_text(getattr(entry, 'summary', '') or getattr(entry, 'description', ''), 300)
            link = getattr(entry, 'link', '')

            if not title:
                continue

            score, layer, access = score_article(title, summary)

            if score >= MIN_SCORE and layer > 0:
                articles.append({
                    "id": article_id(title, link),
                    "title": title,
                    "summary": summary,
                    "company": extract_company(title),
                    "link": link,
                    "date": format_date(entry),
                    "source": source["name"],
                    "layer": layer,
                    "access": access,
                    "score": score,
                })

        print(f"  {source['name']}: {len(articles)} relevant articles")

    except URLError as e:
        print(f"  {source['name']}: fetch failed - {e}")
    except Exception as e:
        print(f"  {source['name']}: error - {e}")

    return articles


# ── MAIN ──────────────────────────────────────────────────────────────

def run():
    print(f"\n=== Experience Economy Scraper — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} ===\n")

    # Load existing events (Option B: append to existing)
    existing = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE) as f:
                data = json.load(f)
                existing = data.get("scraped", [])
            print(f"Loaded {len(existing)} existing scraped events\n")
        except Exception as e:
            print(f"Could not load existing events: {e}\n")

    existing_ids = {e["id"] for e in existing}

    # Fetch all sources
    all_articles = []
    for source in SOURCES:
        print(f"Fetching: {source['name']}...")
        articles = fetch_feed(source)
        all_articles.extend(articles)

    print(f"\nTotal relevant articles found: {len(all_articles)}")

    # Deduplicate
    new_articles = [a for a in all_articles if a["id"] not in existing_ids]
    print(f"New (not in existing): {len(new_articles)}")

    # Sort by score desc
    new_articles.sort(key=lambda x: x["score"], reverse=True)

    # Cap per layer
    layer_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    filtered = []
    for article in new_articles:
        layer = article["layer"]
        if layer_counts.get(layer, 0) < MAX_PER_LAYER:
            filtered.append(article)
            layer_counts[layer] = layer_counts.get(layer, 0) + 1

    print(f"After per-layer cap: {len(filtered)} new events")
    for layer, count in layer_counts.items():
        if count > 0:
            print(f"  Layer {layer}: {count} new events")

    # Merge: new first, then existing, trimmed to MAX_EVENTS
    combined = filtered + existing
    combined = combined[:MAX_EVENTS]

    # Write output
    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "total": len(combined),
        "scraped": combined
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Wrote {len(combined)} events to {OUTPUT_FILE}")
    print("=== Done ===\n")


if __name__ == "__main__":
    run()
