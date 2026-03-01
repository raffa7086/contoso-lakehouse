import os
import sys
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyodbc
from dotenv import load_dotenv


def _load_state(path: Path) -> dict:
    """
    Lê o estado (watermark). Se não existir, retorna {}.
    """
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _save_state(path: Path, state: dict) -> None:
    """
    Salva o estado (watermark) de forma simples.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _atomic_write_parquet(df: pd.DataFrame, final_path: Path) -> None:
    """
    Escrita atômica: escreve tmp e renomeia.
    Evita arquivo final corrompido se cair no meio.
    """
    tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(final_path)


def main() -> int:
    """
    RAW ingest (append-only) para a tabela Data.Sales.
    - Cada execução gera um "lote" (batch) identificável
    """

    # Uso:
    #   python src/extract_raw_sales.py --window-days 7
    window_days = 7
    if "--window-days" in sys.argv:
        window_days = int(sys.argv[sys.argv.index("--window-days") + 1])

    base_dir = Path(__file__).resolve().parent.parent

    # RAW landing root (separado do que você tinha antes)
    landing_root = base_dir / "landing" / "raw" / "Data_Sales"
    landing_root.mkdir(parents=True, exist_ok=True)

    # Estado do watermark (continua existindo, mas agora controla o que buscar do SQL)
    state_path = base_dir / "state" / "Data_Sales_raw.json"
    state = _load_state(state_path)
    last_order_date = state.get("last_order_date", "1900-01-01")

    last_dt = datetime.strptime(last_order_date, "%Y-%m-%d").date()
    start_dt = last_dt - timedelta(days=window_days)
    start_str = start_dt.strftime("%Y-%m-%d")

    # Credenciais
    load_dotenv(base_dir / ".env")
    password = os.getenv("MSSQL_INGEST_PASSWORD")
    if not password:
        print("ERROR: MSSQL_INGEST_PASSWORD não encontrado no .env")
        return 2

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=ContosoV2_1M;"
        "UID=ingest_ro;"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )

    # Query: pega somente janela (por OrderDate) — isso é seu “incremental por data”
    query = f"SELECT * FROM Data.Sales WHERE OrderDate >= '{start_str}'"

    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        print(f"OK (raw): no rows since {start_str}. Nothing to do.")
        return 0

    # Enriquecimento RAW: adicionar metadados de ingestão (auditabilidade)
    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df["_ingest_batch_ts_utc"] = batch_ts
    df["_ingest_window_start"] = start_str

    # Layout RAW: particionado por dia + batch
    # landing/raw/Data_Sales/OrderDate=YYYY-MM-DD/batch=YYYYMMDDTHHMMSSZ/data.parquet
    max_order_date = pd.to_datetime(df["OrderDate"]).dt.date.max()

    # Para não misturar dias dentro de um único arquivo:
    # vamos escrever 1 arquivo por dia por batch
    written_days = 0
    for day, df_day in df.groupby(pd.to_datetime(df["OrderDate"]).dt.date):
        day_str = pd.to_datetime(day).strftime("%Y-%m-%d")
        out_dir = landing_root / f"OrderDate={day_str}" / f"batch={batch_ts}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "data.parquet"
        _atomic_write_parquet(df_day, out_path)
        written_days += 1

    # Atualiza watermark para a maior OrderDate vista nesse pull
    _save_state(state_path, {"last_order_date": max_order_date.strftime("%Y-%m-%d")})

    print(
        "OK (raw): "
        f"root={landing_root} rows_in_window={len(df)} "
        f"days_written={written_days} watermark={max_order_date} window_start={start_str} batch={batch_ts}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())