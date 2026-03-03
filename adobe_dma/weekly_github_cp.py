import os
from pathlib import Path
from datetime import timedelta
from adobe_dma.core_cp import (
    AdobeAAClient,
    prior_completed_week_sun_to_sun,
    upload_to_ftp,
)

def main():
    # ---------------- Adobe credentials (GitHub Secrets -> env) ----------------
    AA_CLIENT_ID = os.environ["AA_CLIENT_ID"]
    AA_CLIENT_SECRET = os.environ["AA_CLIENT_SECRET"]
    AA_API_KEY = os.environ.get("AA_API_KEY", AA_CLIENT_ID)
    AA_ORG_ID = os.environ["AA_ORG_ID"]
    AA_COMPANY_ID = os.environ["AA_COMPANY_ID"]

    # ---------------- Brand selection ----------------
    brand_keys = [b.strip().upper() for b in os.environ.get("BRAND_KEYS", "CP").split(",") if b.strip()]

    # ---------------- Performance knobs (safe defaults for GitHub) ----------------
    page_size = int(os.environ.get("PAGE_SIZE", "2000"))
    workers_pages = int(os.environ.get("WORKERS_PAGES", "8"))
    workers_segments = int(os.environ.get("WORKERS_SEGMENTS", "5"))

    # ---------------- Local dirs ----------------
    output_dir = Path("output")
    cache_root = Path(".aa_cache")
    output_dir.mkdir(exist_ok=True, parents=True)
    cache_root.mkdir(exist_ok=True, parents=True)

    # ---------------- Create client ----------------
    client = AdobeAAClient(
        client_id=AA_CLIENT_ID,
        client_secret=AA_CLIENT_SECRET,
        api_key=AA_API_KEY,
        org_id=AA_ORG_ID,
        company_id=AA_COMPANY_ID,
        page_size=page_size,
        workers_pages=workers_pages,
        workers_segments=workers_segments,
        cache_root=cache_root,
        output_dir=output_dir,
    )

    # ---------------- Date range ----------------
    ws, we = prior_completed_week_sun_to_sun()
    fiscal_end_sat = we - timedelta(days=1)
    print(f"🗓️ Running fiscal week (Sun-Sat): {ws} to {fiscal_end_sat}")
    print(f"   (Adobe API dateRange end-exclusive): {ws}T00:00:00.000/{we}T00:00:00.000")

    # ---------------- Run report ----------------
    out_path = client.run_week_to_excel(
        brand_keys=brand_keys,
        start_sun=ws,
        end_sun_excl=we,
        filename=None,
        show_progress=True,
    )
    print(f"✅ Created: {out_path}")

    # ---------------- FTP upload ----------------
    ftp_host = os.environ.get("FTP_HOST", "")
    ftp_user = os.environ.get("FTP_USER", "")
    ftp_pass = os.environ.get("FTP_PASS", "")
    ftp_dir  = os.environ.get("FTP_DIR", "/")
    ftp_tls  = os.environ.get("FTP_TLS", "true").lower() in {"1", "true", "yes"}

    if ftp_host and ftp_user and ftp_pass:
        upload_to_ftp(out_path, ftp_dir, ftp_host, ftp_user, ftp_pass, try_ftps=ftp_tls, retries=3)
    else:
        print("ℹ️ FTP upload skipped (FTP_HOST/FTP_USER/FTP_PASS not set).")

    # ---------------- Helpful pointer file ----------------
    (output_dir / "latest.txt").write_text(str(out_path.resolve()), encoding="utf-8")
    print(f"📝 Wrote pointer: {output_dir / 'latest.txt'}")

if __name__ == "__main__":
    main()
