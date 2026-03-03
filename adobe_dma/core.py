import time
import json
import threading
import ftplib
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date

import requests
import pandas as pd
from tqdm.auto import tqdm

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # python<3.9

# ================== Global constants ==================

TZ = ZoneInfo("America/New_York")
OAUTH_SCOPES = "openid,AdobeID,read_organizations,additional_info.projectedProductContext,session"

OUTPUT_COLUMNS = ["Date", "DMA", "Last_Touch_Channel", "Visits", "Orders", "Demand", "NMA", "NTF"]

VISITS_ID = "metrics/visits"
ORDERS_ID = "metrics/orders"
DEMAND_ID = "cm300006919_5a132314ae06387a6a2fec85"
NMA_ID = "metrics/event233"
NTF_ID = "metrics/event263"
DIMENSION = "variables/geodma"

# ================== Brand configs ==================
# KS only (easy to add WW later if needed)

KS_SEGMENTS: Dict[str, str] = {
    "s300006919_67dc7a83eb5ca74056703483": "Affiliate",
    "s300006919_67dc7ab45576732634ebe466": "Cross Brand",
    "s300006919_67dc7a962d5fdc38d3a9ad2b": "CSE",
    "s300006919_67dc7ac7216b535a07c6b208": "Direct to Site",
    "s300006919_67dc7ada5576732634ebe467": "Display - Retargeting",
    "s300006919_67dc7af72d5fdc38d3a9ad2d": "Email",
    "s300006919_67dc7b06eb5ca74056703487": "Facebook Advantage",
    "s300006919_67dc7b17eb5ca74056703488": "Natural Search",
    "s300006919_67dc7b42eb5ca7405670348a": "Paid Search - Brand",
    "s300006919_67dc7b5e5576732634ebe46b": "Paid Search - Non Brand",
    "s300006919_67dc7b6beb5ca7405670348c": "Paid Search - PLA",
    "s300006919_67dc7b89eb5ca7405670348e": "SMS",
    "s300006919_67dc7b9c5576732634ebe46d": "Social - Organic",
    "s300006919_67dc7bab2d5fdc38d3a9ad34": "Social - Prospecting",
    # IMPORTANT: this is your provided reactivation segment id:
    "s300006919_67dc7bbc2d5fdc38d3a9ad36": "Social - Reactivation",
    "s300006919_67dc7bcc2d5fdc38d3a9ad38": "Social - Retargeting",
    "s300006919_67dc7bdd5576732634ebe46f": "Social - Retention",
    "s300006919_67dc7bf4eb5ca74056703492": "Unspecified",
}

BRANDS: Dict[str, Dict[str, Any]] = {
    "KS": {"rsid": "vrs_ospgro1_kingsize", "segments": KS_SEGMENTS},
}

# ================== Date helpers ==================

def prior_completed_week_sun_to_sun(now_et: Optional[datetime] = None) -> Tuple[date, date]:
    """
    Returns (start_sunday, end_sunday_exclusive) for the PRIOR completed week.
    Fiscal week is Sunday->Saturday inclusive, end is exclusive (next Sunday).
    """
    now = now_et or datetime.now(TZ)
    days_since_sunday = (now.weekday() + 1) % 7
    last_sunday = (now - timedelta(days=days_since_sunday)).date()
    start_sun = last_sunday - timedelta(days=7)
    end_sun_excl = last_sunday
    return start_sun, end_sun_excl

def stable_week_filename(brand_keys: List[str], start_sun: str, end_sat: str) -> str:
    brands_part = "_".join(brand_keys)
    return f"adobe_dma_by_segment_{brands_part}_{start_sun}_to_{end_sat}.xlsx"

def daterange_days(start_day: date, end_day_excl: date) -> List[date]:
    """Return list of dates: start_day .. end_day_excl-1"""
    out = []
    cur = start_day
    while cur < end_day_excl:
        out.append(cur)
        cur += timedelta(days=1)
    return out

def day_iso_range(d: date) -> Tuple[str, str]:
    """Single day range: d 00:00 to d+1 00:00 (exclusive)."""
    start_iso = f"{d}T00:00:00.000"
    end_iso = f"{(d + timedelta(days=1))}T00:00:00.000"
    return start_iso, end_iso

# ================== Token manager ==================

class TokenManager:
    def __init__(self, client_id: str, client_secret: str, auth_url: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_url = auth_url
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def _request_new_token(self) -> Tuple[str, float]:
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": OAUTH_SCOPES
        }
        resp = requests.post(self.auth_url, data=data, timeout=30)
        if not resp.ok:
            raise RuntimeError(f"IMS token request failed: {resp.status_code} {resp.text}")
        payload = resp.json()
        token = payload["access_token"]
        expires_at = time.time() + int(payload.get("expires_in", 3600))
        return token, expires_at

    def get_token(self) -> str:
        with self._lock:
            if self._token and time.time() < self._expires_at - 30:
                return self._token
            self._token, self._expires_at = self._request_new_token()
            return self._token

    def force_refresh(self) -> str:
        with self._lock:
            self._token, self._expires_at = self._request_new_token()
            return self._token

# ================== Adobe AA client ==================

class AdobeAAClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        api_key: str,
        org_id: str,
        company_id: str,
        page_size: int = 2000,
        workers_pages: int = 8,
        workers_segments: int = 5,
        cache_root: Path = Path(".aa_cache"),
        output_dir: Path = Path("output"),
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_key = api_key
        self.org_id = org_id
        self.company_id = company_id

        self.page_size = page_size
        self.workers_pages = workers_pages
        self.workers_segments = workers_segments

        self.auth_url = "https://ims-na1.adobelogin.com/ims/token/v3"
        self.report_url = f"https://analytics.adobe.io/api/{company_id}/reports"

        self.cache_root = cache_root
        self.cache_root.mkdir(parents=True, exist_ok=True)

        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.token_mgr = TokenManager(client_id, client_secret, self.auth_url)

    def _headers(self) -> Dict[str, str]:
        token = self.token_mgr.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "x-api-key": self.api_key,
            "x-gw-ims-org-id": self.org_id,
            "x-proxy-global-company-id": self.company_id,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _sleep_backoff(self, attempt: int, base: float = 0.75, cap: float = 20.0):
        delay = min(cap, base * (2 ** attempt) + 0.05 * attempt)
        time.sleep(delay)

    def _build_payload(
        self,
        rsid: str,
        segment_id: str,
        start_iso: str,
        end_iso: str,
        page: int,
        limit: int
    ) -> Dict[str, Any]:
        return {
            "rsid": rsid,
            "globalFilters": [
                {"type": "segment", "segmentId": segment_id},
                {"type": "dateRange", "dateRange": f"{start_iso}/{end_iso}"}
            ],
            "metricContainer": {
                "metrics": [
                    {"columnId": "0", "id": VISITS_ID, "sort": "desc"},
                    {"columnId": "1", "id": ORDERS_ID},
                    {"columnId": "2", "id": DEMAND_ID},
                    {"columnId": "3", "id": NMA_ID},
                    {"columnId": "4", "id": NTF_ID},
                ]
            },
            "dimension": DIMENSION,
            "settings": {
                "countRepeatInstances": True,
                "includeAnnotations": True,
                "limit": limit,
                "page": page,
                "nonesBehavior": "return-nones"
            }
        }

    def _parse_rows(self, rows: List[Dict[str, Any]], date_label: str, channel_name: str) -> pd.DataFrame:
        out = []
        for r in rows or []:
            vals = r.get("data", [])
            out.append({
                "Date": date_label,  # single day YYYY-MM-DD
                "DMA": r.get("value"),
                "Last_Touch_Channel": channel_name,
                "Visits": vals[0] if len(vals) > 0 else None,
                "Orders": vals[1] if len(vals) > 1 else None,
                "Demand": vals[2] if len(vals) > 2 else None,
                "NMA": vals[3] if len(vals) > 3 else None,
                "NTF": vals[4] if len(vals) > 4 else None,
            })
        return pd.DataFrame(out, columns=OUTPUT_COLUMNS)

    def _fetch_page(
        self,
        session: requests.Session,
        cache_root: Path,
        rsid: str,
        segment_id: str,
        start_iso: str,
        end_iso: str,
        page: int
    ) -> Dict[str, Any]:
        seg_dir = cache_root / rsid / segment_id
        seg_dir.mkdir(parents=True, exist_ok=True)
        cache_file = seg_dir / f"page_{page:05d}.json"

        if cache_file.exists():
            with cache_file.open("r", encoding="utf-8") as f:
                return json.load(f)

        payload = self._build_payload(rsid, segment_id, start_iso, end_iso, page, self.page_size)
        max_attempts = 6
        attempt = 0

        while attempt < max_attempts:
            try:
                resp = session.post(self.report_url, headers=self._headers(), json=payload, timeout=120)
                status = resp.status_code

                if status == 200:
                    data = resp.json()
                    with cache_file.open("w", encoding="utf-8") as f:
                        json.dump(data, f)
                    return data

                if status == 401:
                    self.token_mgr.force_refresh()
                    attempt += 1
                    self._sleep_backoff(attempt)
                    continue

                if status in (429, 500, 502, 503, 504):
                    ra = resp.headers.get("Retry-After")
                    attempt += 1
                    if ra:
                        try:
                            time.sleep(float(ra))
                        except Exception:
                            self._sleep_backoff(attempt)
                    else:
                        self._sleep_backoff(attempt)
                    continue

                raise RuntimeError(f"HTTP {status} rsid={rsid} seg={segment_id} page={page}: {resp.text}")

            except requests.Timeout:
                attempt += 1
                self._sleep_backoff(attempt)
            except requests.RequestException:
                attempt += 1
                self._sleep_backoff(attempt)

        raise RuntimeError(f"Failed rsid={rsid} seg={segment_id} page={page} after {max_attempts} attempts")

    def _fetch_segment_all_pages_for_day(
        self,
        day_cache_root: Path,
        rsid: str,
        segment_id: str,
        channel_name: str,
        start_iso: str,
        end_iso: str,
        date_label: str,
    ) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        seg_dir = day_cache_root / rsid / segment_id
        seg_dir.mkdir(parents=True, exist_ok=True)

        with requests.Session() as session:
            first = self._fetch_page(session, day_cache_root, rsid, segment_id, start_iso, end_iso, 0)
            total_pages = int(first.get("totalPages", 1))
            frames.append(self._parse_rows(first.get("rows", []), date_label, channel_name))

            cached_pages = {
                int(p.stem.split("_")[-1]) for p in seg_dir.glob("page_*.json")
                if p.stem.split("_")[-1].isdigit()
            }
            needed = [p for p in range(1, total_pages) if p not in cached_pages]

            if needed:
                with ThreadPoolExecutor(max_workers=self.workers_pages) as pool:
                    futs = {
                        pool.submit(
                            self._fetch_page,
                            session,
                            day_cache_root,
                            rsid,
                            segment_id,
                            start_iso,
                            end_iso,
                            p
                        ): p
                        for p in needed
                    }
                    for fut in as_completed(futs):
                        data = fut.result()
                        frames.append(self._parse_rows(data.get("rows", []), date_label, channel_name))

        if frames:
            return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    def run_week_to_excel(
        self,
        brand_keys: List[str],
        start_sun: date,
        end_sun_excl: date,
        filename: Optional[str] = None,
        show_progress: bool = True,
    ) -> Path:
        for bkey in brand_keys:
            if bkey not in BRANDS:
                raise ValueError(f"Unknown brand '{bkey}'. Valid: {sorted(BRANDS.keys())}")

        # Weekly filename uses Sun->Sat (nice and matches fiscal week display)
        end_sat = end_sun_excl - timedelta(days=1)
        label_start = str(start_sun)
        label_end_sat = str(end_sat)

        if not filename:
            filename = stable_week_filename(brand_keys, label_start, label_end_sat)

        out_path = self.output_dir / filename

        # Pull each day in the fiscal week
        days = daterange_days(start_sun, end_sun_excl)

        problems: List[str] = []
        had_any_sheet = False

        with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as writer:
            for bkey in brand_keys:
                rsid = BRANDS[bkey]["rsid"]
                segments: Dict[str, str] = BRANDS[bkey]["segments"]
                seg_items = list(segments.items())

                frames_for_brand: List[pd.DataFrame] = []

                day_iter = days
                if show_progress:
                    day_iter = tqdm(days, desc=f"Days ({bkey})", leave=True)

                for d in day_iter:
                    start_iso, end_iso = day_iso_range(d)
                    date_label = str(d)  # <- daily Date value

                    # day-scoped cache -> resumable per day
                    day_cache_root = self.cache_root / f"{d}"
                    day_cache_root.mkdir(parents=True, exist_ok=True)

                    with ThreadPoolExecutor(max_workers=self.workers_segments) as pool:
                        futs = {
                            pool.submit(
                                self._fetch_segment_all_pages_for_day,
                                day_cache_root,
                                rsid,
                                seg_id,
                                ch_name,
                                start_iso,
                                end_iso,
                                date_label,
                            ): seg_id
                            for seg_id, ch_name in seg_items
                        }

                        pbar = tqdm(total=len(futs), desc=f"Segments ({bkey} {d})", leave=False) if show_progress else None

                        for fut in as_completed(futs):
                            seg_id = futs[fut]
                            try:
                                df = fut.result()
                                if not df.empty:
                                    frames_for_brand.append(df)
                            except Exception as e:
                                problems.append(f"{bkey}/{rsid} | {d} | {seg_id}: {e}")
                            finally:
                                if pbar:
                                    pbar.update(1)

                        if pbar:
                            pbar.close()

                if frames_for_brand:
                    df_brand = pd.concat(frames_for_brand, ignore_index=True)[OUTPUT_COLUMNS]

                    # ---- SORTING (requested) ----
                    # Date asc, Channel asc, Visits desc, DMA asc
                    df_brand = df_brand.sort_values(
                        by=["Date", "Last_Touch_Channel", "Visits", "DMA"],
                        ascending=[True, True, False, True],
                        kind="mergesort",
                        na_position="last",
                    )

                    sheet_name = f"{bkey}_{rsid}"[:31]
                    df_brand.to_excel(writer, sheet_name=sheet_name, index=False)
                    had_any_sheet = True
                else:
                    problems.append(f"{bkey}/{rsid}: no data collected for any day/segment.")

            if not had_any_sheet:
                pd.DataFrame(columns=OUTPUT_COLUMNS).to_excel(writer, sheet_name="Summary", index=False)
                if problems:
                    pd.DataFrame({"Issues": problems}).to_excel(writer, sheet_name="Errors", index=False)

        if problems:
            print("⚠️ Issues encountered:")
            for p in problems:
                print(" -", p)

        return out_path

# ================== FTP upload ==================

def upload_to_ftp(local_path: Path, remote_dir: str, host: str, user: str, pwd: str, try_ftps: bool = True, retries: int = 3):
    def _ensure_dir(ftp, d):
        parts = [p for p in d.split("/") if p]
        for p in parts:
            try:
                ftp.cwd(p)
            except Exception:
                ftp.mkd(p)
                ftp.cwd(p)

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            if try_ftps:
                ftp = ftplib.FTP_TLS(timeout=60)
                ftp.connect(host, 21)
                ftp.login(user=user, passwd=pwd)
                ftp.prot_p()
            else:
                ftp = ftplib.FTP(timeout=60)
                ftp.connect(host, 21)
                ftp.login(user=user, passwd=pwd)

            ftp.set_pasv(True)

            if remote_dir and remote_dir != "/":
                try:
                    ftp.cwd("/")
                except Exception:
                    pass
                _ensure_dir(ftp, remote_dir)

            with open(local_path, "rb") as fh:
                ftp.storbinary(f"STOR {local_path.name}", fh)

            try:
                ftp.quit()
            except Exception:
                pass

            print(f"✅ FTP upload complete: {host}:{remote_dir}/{local_path.name}")
            return

        except Exception as e:
            last_err = e
            print(f"[FTP attempt {attempt}/{retries}] {type(e).__name__}: {e}")
            try_ftps = False
            time.sleep(min(10, 2 * attempt))

    raise RuntimeError(f"FTP upload failed after {retries} attempts: {last_err}")
