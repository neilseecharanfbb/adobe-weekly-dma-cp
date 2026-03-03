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
    from backports.zoneinfo import ZoneInfo


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


# ================== CP Brand Config ==================

CP_SEGMENTS: Dict[str, str] = {
    "s300006919_67d31cd1216b535a07c6832a": "Affiliate",
    "s300006919_67d31961863e7c14e2170487": "Cross Brand",
    "s300006919_67d31c5eeb5ca7405670058e": "CSE",
    "s300006919_67d31b8f5576732634ebb51a": "Direct to Site",
    "s300006919_67dd7a8724a81279e930b0b0": "Display - Retargeting",
    "s300006919_67d31aea2d5fdc38d3a97e01": "Email",
    "s300006919_67d31a6d5576732634ebb507": "Facebook Advantage",
    "s300006919_67d31be9eb5ca74056700587": "Natural Search",
    "s300006919_67d31aa9eb5ca74056700574": "Paid Search - Brand",
    "s300006919_67d31c99eb5ca74056700591": "Paid Search - Non Brand",
    "s300006919_67d31b35eb5ca74056700580": "Paid Search - PLA",
    "s300006919_67d31c30eb5ca7405670058a": "SMS",
    "s300006919_67d31d125576732634ebb530": "Social - Organic",
    "s300006919_67d31db82d5fdc38d3a97e25": "Social - Prospecting",
    "s300006919_67d31d4c216b535a07c68331": "Social - Reactivation",
    "s300006919_67d31e07216b535a07c6833a": "Social - Retargeting",
    "s300006919_67d31d89eb5ca74056700596": "Social - Retention",
    "s300006919_67d319b829ae424c05d29053": "Unspecified",
}

BRANDS: Dict[str, Dict[str, Any]] = {
    "CP": {
        "rsid": "vrs_ospgro1_eloquiicopy_0",
        "segments": CP_SEGMENTS,
    }
}


# ================== Date helpers ==================

def prior_completed_week_sun_to_sun(now_et: Optional[datetime] = None) -> Tuple[date, date]:
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
    out = []
    cur = start_day
    while cur < end_day_excl:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def day_iso_range(d: date) -> Tuple[str, str]:
    return f"{d}T00:00:00.000", f"{(d + timedelta(days=1))}T00:00:00.000"


# ================== Token Manager ==================

class TokenManager:
    def __init__(self, client_id: str, client_secret: str, auth_url: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_url = auth_url
        self._token = None
        self._expires_at = 0
        self._lock = threading.Lock()

    def _request_new_token(self):
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

    def get_token(self):
        with self._lock:
            if self._token and time.time() < self._expires_at - 30:
                return self._token
            self._token, self._expires_at = self._request_new_token()
            return self._token

    def force_refresh(self):
        with self._lock:
            self._token, self._expires_at = self._request_new_token()
            return self._token


# ================== Adobe AA Client ==================

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

    def _headers(self):
        token = self.token_mgr.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "x-api-key": self.api_key,
            "x-gw-ims-org-id": self.org_id,
            "x-proxy-global-company-id": self.company_id,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _sleep_backoff(self, attempt: int):
        time.sleep(min(20, 0.75 * (2 ** attempt)))

    def _build_payload(self, rsid, segment_id, start_iso, end_iso, page):
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
                "limit": self.page_size,
                "page": page,
                "nonesBehavior": "return-nones"
            }
        }

    def _parse_rows(self, rows, date_label, channel_name):
        out = []
        for r in rows or []:
            vals = r.get("data", [])
            out.append({
                "Date": date_label,
                "DMA": r.get("value"),
                "Last_Touch_Channel": channel_name,
                "Visits": vals[0] if len(vals) > 0 else None,
                "Orders": vals[1] if len(vals) > 1 else None,
                "Demand": vals[2] if len(vals) > 2 else None,
                "NMA": vals[3] if len(vals) > 3 else None,
                "NTF": vals[4] if len(vals) > 4 else None,
            })
        return pd.DataFrame(out, columns=OUTPUT_COLUMNS)

    def run_week_to_excel(self, brand_keys, start_sun, end_sun_excl):
        days = daterange_days(start_sun, end_sun_excl)
        end_sat = end_sun_excl - timedelta(days=1)
        filename = stable_week_filename(brand_keys, str(start_sun), str(end_sat))
        out_path = self.output_dir / filename

        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for bkey in brand_keys:
                rsid = BRANDS[bkey]["rsid"]
                segments = BRANDS[bkey]["segments"]
                frames = []

                for d in tqdm(days, desc=f"Days ({bkey})"):
                    start_iso, end_iso = day_iso_range(d)
                    date_label = str(d)

                    for seg_id, channel_name in segments.items():
                        page = 0
                        while True:
                            payload = self._build_payload(rsid, seg_id, start_iso, end_iso, page)
                            resp = requests.post(self.report_url, headers=self._headers(), json=payload)

                            if not resp.ok:
                                raise RuntimeError(resp.text)

                            data = resp.json()
                            rows = data.get("rows", [])
                            if not rows:
                                break

                            df = self._parse_rows(rows, date_label, channel_name)
                            frames.append(df)

                            if page + 1 >= data.get("totalPages", 1):
                                break
                            page += 1

                if frames:
                    df_brand = pd.concat(frames, ignore_index=True)
                    df_brand = df_brand.sort_values(
                        by=["Date", "Last_Touch_Channel", "Visits", "DMA"],
                        ascending=[True, True, False, True],
                        kind="mergesort",
                        na_position="last",
                    )
                    sheet_name = f"{bkey}_{rsid}"[:31]
                    df_brand.to_excel(writer, sheet_name=sheet_name, index=False)

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
