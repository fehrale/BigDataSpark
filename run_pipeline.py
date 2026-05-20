from __future__ import annotations

import argparse
import base64
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def compose_file() -> str:
    f = ROOT / "docker-compose.yml"
    if not f.is_file():
        raise FileNotFoundError(f"docker-compose.yml not found at {f}")
    return str(f)


def run_compose(argv: list[str]) -> None:
    cmd = ["docker", "compose", "-f", compose_file()] + argv
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def wait_postgres(timeout_s: float = 120) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        r = subprocess.run(
            ["docker", "compose", "-f", compose_file(), "exec", "-T", "postgres", "pg_isready", "-U", "user", "-d", "snowflake"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if r.returncode == 0:
            print("postgres ready")
            return
        time.sleep(2)
    raise TimeoutError("PostgreSQL did not become ready")


def wait_fact_ready(timeout_s: float = 240) -> None:
    deadline = time.monotonic() + timeout_s
    q = "SELECT COUNT(*) FROM snowflake.fact_transactions"
    while time.monotonic() < deadline:
        r = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                compose_file(),
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                "user",
                "-d",
                "snowflake",
                "-tAc",
                q,
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if r.returncode == 0 and r.stdout.strip().isdigit() and int(r.stdout.strip()) > 0:
            print(f"snowflake fact_transactions rows={r.stdout.strip()} (init done)")
            return
        time.sleep(3)
    raise TimeoutError("snowflake.fact_transactions did not load in time (docker init still running?)")


def clickhouse_scalar(
    query: str,
    host: str = "localhost",
    port: int = 8123,
    user: str | None = None,
    password: str | None = None,
) -> str:
    u = os.environ.get("CLICKHOUSE_USER", "default") if user is None else user
    p = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse") if password is None else password
    q = urllib.parse.quote_plus(query.strip())
    url = f"http://{host}:{port}/?default_format=TabSeparatedRaw&query={q}"
    req = urllib.request.Request(url)
    if p:
        token = base64.b64encode(f"{u}:{p}".encode()).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(req, timeout=30) as r:
        body = r.read().decode().strip()
    return body


def wait_clickhouse(timeout_s: float = 90) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if clickhouse_scalar("SELECT 1") == "1":
                print("clickhouse ping ok")
                return
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(2)
    raise TimeoutError("ClickHouse did not become ready")


def exec_spark(script: str) -> None:
    wf = ROOT / script
    if not wf.is_file():
        raise FileNotFoundError(f"Missing Spark job {wf}")

    work = "/home/jovyan/work"
    wf_inner = f"{work}/{wf.name}"
    cmd = [
        "docker",
        "compose",
        "-f",
        compose_file(),
        "exec",
        "-T",
        "spark",
        "spark-submit",
        "--packages",
        "org.postgresql:postgresql:42.7.4,com.clickhouse:clickhouse-jdbc:0.4.6",
        wf_inner,
    ]
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full ETL (Docker Compose + Spark jobs)")
    parser.add_argument("--skip-up", action="store_true", help="Do not docker compose up -d")
    parser.add_argument("--skip-etl-star", action="store_true")
    parser.add_argument("--skip-etl-clickhouse", action="store_true")
    parser.add_argument("--skip-smoke", action="store_true")
    args = parser.parse_args()

    if not args.skip_up:
        run_compose(["up", "-d", "--build"])
        wait_postgres()
        wait_fact_ready()
        wait_clickhouse()

    if not args.skip_etl_star:
        exec_spark("etl_star_schema.py")

    if not args.skip_etl_clickhouse:
        exec_spark("etl_clickhouse_reports.py")

    if not args.skip_smoke:
        for t in (
            "report_products_top10",
            "report_products_by_category",
            "report_products_ratings",
            "report_customers_top10",
            "report_customers_by_country",
            "report_customers_avg_check",
            "report_time_monthly",
            "report_time_yearly",
            "report_time_weekday",
            "report_stores_top5",
            "report_stores_by_city",
            "report_stores_by_country",
            "report_suppliers_top5",
            "report_suppliers_avg_price",
            "report_suppliers_by_country",
            "report_quality_top_rated",
            "report_quality_low_rated",
            "report_quality_most_reviewed",
        ):
            cnt = clickhouse_scalar(f"SELECT COUNT(*) FROM {t}")
            print(f"CHK {t} rows={cnt}")
            if not cnt.strip().isdigit() or int(cnt.strip()) == 0:
                raise SystemExit(f"smoke failed for {t}: {cnt!r}")
            print(f"{t} smoke ok")

    print("pipeline OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
