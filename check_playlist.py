import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ─── CONFIG ──────────────────────────────────────────────────────────
SOURCES_FILE = "sources.txt"
OUTPUT_FILE  = "ridoymovieserver.m3u"
TIMEOUT      = 10    # seconds per stream check
MAX_WORKERS  = 50    # how many streams to check in parallel
# ─────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def read_sources():
    if not os.path.exists(SOURCES_FILE):
        print(f"ERROR: {SOURCES_FILE} not found!")
        return []
    with open(SOURCES_FILE, encoding="utf-8") as f:
        return [
            line.strip() for line in f
            if line.strip() and not line.startswith("#")
        ]


def fetch_m3u(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  WARNING: Could not fetch {url}  ->  {e}")
        return None


def parse_m3u(text):
    entries = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.upper().startswith("#EXTINF"):
            extinf = line
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines):
                candidate = lines[j].strip()
                if candidate and not candidate.startswith("#"):
                    entries.append((extinf, candidate))
                i = j + 1
                continue
        i += 1
    return entries


def is_alive(url):
    try:
        r = requests.head(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code in (200, 206):
            return True
        if r.status_code in (400, 405, 500):
            raise requests.exceptions.RequestException("HEAD blocked, trying GET")
        return False
    except Exception:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True)
            r.close()
            return r.status_code in (200, 206)
        except Exception:
            return False


def main():
    sep = "=" * 58
    print()
    print(sep)
    print("  RidoyMovieServer  –  Auto Playlist Updater")
    print(f"  Started : {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(sep)
    print()

    sources = read_sources()
    if not sources:
        print("No sources configured. Add URLs to sources.txt and re-run.")
        return

    all_entries, seen = [], set()
    for src in sources:
        print(f"Downloading: {src}")
        text = fetch_m3u(src)
        if not text:
            continue
        parsed = parse_m3u(text)
        added = 0
        for extinf, url in parsed:
            if url not in seen:
                all_entries.append((extinf, url))
                seen.add(url)
                added += 1
        print(f"  -> {added} unique entries  (total: {len(all_entries)})")

    print()
    print(f"Checking {len(all_entries)} streams with {MAX_WORKERS} parallel workers ...")
    print()

    live = []
    dead = 0
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fmap = {ex.submit(is_alive, u): (inf, u) for inf, u in all_entries}
        for fut in as_completed(fmap):
            inf, url = fmap[fut]
            done += 1
            ok = False
            try:
                ok = fut.result()
            except Exception:
                pass
            if ok:
                live.append((inf, url))
            else:
                dead += 1
            if done % 100 == 0 or done == len(all_entries):
                pct = done / len(all_entries) * 100
                print(f"  {done}/{len(all_entries)} ({pct:.0f}%) | alive={len(live)}  dead={dead}")

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        print("#EXTM3U", file=f)
        print("#PLAYLIST RidoyMovieServer", file=f)
        print(f"# Auto-generated  :  {now_str}", file=f)
        print(f"# Working streams :  {len(live)}", file=f)
        print("", file=f)
        for extinf, url in live:
            print(extinf, file=f)
            print(url, file=f)

    print()
    print(sep)
    print(f"  RESULT  :  {len(live)} working  |  {dead} removed")
    print(f"  Output  :  {OUTPUT_FILE}")
    print(f"  Done    :  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(sep)
    print()


if __name__ == "__main__":
    main()
