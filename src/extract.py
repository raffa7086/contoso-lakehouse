import json                # Ler e escrever o arquivo de estado (watermark)
import os                  # Ler variáveis de ambiente
import sys                 # Ler argumentos passados via linha de comando
from datetime import datetime, timedelta  # Manipular datas no incremental
from pathlib import Path   # Trabalhar com caminhos

import pandas as pd
import pyodbc              # Driver de conexão com SQL Server
from dotenv import load_dotenv  # Carregar variáveis do arquivo .env


# =========================
def _load_state(path: Path) -> dict:
    """
    Lê o arquivo JSON que guarda o watermark (last_order_date).
    Se o arquivo não existir, retorna dicionário vazio.
    """
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


# =========================
def _save_state(path: Path, state: dict) -> None:
    """
    Salva o watermark atualizado no arquivo JSON.
    Garante que a pasta exista antes de escrever.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


# =========================
def _atomic_write_parquet(df: pd.DataFrame, final_path: Path) -> None:
    """
    Escreve o Parquet primeiro em um arquivo temporário.
    Depois renomeia para o nome final.

    Isso evita corromper o arquivo final se o processo falhar no meio.
    """
    tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(final_path)


# =========================
def main() -> int:
    """
    Ponto de entrada do script.

    Permite dois modos:
      - full (qualquer tabela)
      - incremental (apenas para [Data].[Sales])
    """

    # -------------------------
    # Validação de argumentos
    # -------------------------
    if len(sys.argv) < 2:
        print("Usage: python src/extract.py [schema].[table] [--mode full|incremental] [--window-days N]")
        print("Example: python src/extract.py [Data].[Sales] --mode incremental --window-days 7")
        return 1

    table = sys.argv[1]     # Nome da tabela passado via CLI
    mode = "full"           # Modo padrão é full
    window_days = 7         # Janela padrão para incremental

    # Permite sobrescrever modo via argumento
    if "--mode" in sys.argv:
        mode = sys.argv[sys.argv.index("--mode") + 1].strip().lower()

    # Permite sobrescrever janela
    if "--window-days" in sys.argv:
        window_days = int(sys.argv[sys.argv.index("--window-days") + 1])

    # -------------------------
    # Resolver diretórios
    # -------------------------
    base_dir = Path(__file__).resolve().parent.parent
    landing_dir = base_dir / "landing"
    landing_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Carregar credenciais
    # -------------------------
    load_dotenv(base_dir / ".env")
    password = os.getenv("MSSQL_INGEST_PASSWORD")

    if not password:
        print("ERROR: MSSQL_INGEST_PASSWORD não encontrado no .env")
        return 2

    # -------------------------
    # String de conexão
    # -------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=ContosoV2_1M;"
        "UID=ingest_ro;"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )

    # Nome do arquivo de saída baseado na tabela (modo FULL usa isso)
    output_file = table.replace("[", "").replace("]", "").replace(".", "_") + ".parquet"
    output_path = landing_dir / output_file

    # =========================
    # MODO FULL
    # =========================
    if mode == "full":
        """
        Lê a tabela inteira e sobrescreve o parquet (1 arquivo por tabela).
        """
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)

        _atomic_write_parquet(df, output_path)

        print(f"OK (full): {output_path} rows={len(df)}")
        return 0

    # =========================
    # Validação modo incremental
    # =========================
    if mode != "incremental":
        print("ERROR: --mode must be full or incremental")
        return 3

    # Neste projeto, incremental só foi implementado para Sales
    if table.lower() != "[data].[sales]":
        print("ERROR: incremental mode is implemented only for [Data].[Sales] in this project.")
        return 4

    # =========================
    # Carregar watermark
    # =========================
    state_path = base_dir / "state" / "Data_Sales.json"
    state = _load_state(state_path)

    # Se não existir, assume 1900-01-01
    last_order_date = state.get("last_order_date", "1900-01-01")

    # Converter string para date
    last_dt = datetime.strptime(last_order_date, "%Y-%m-%d").date()

    # Aplicar janela de segurança (reprocessamento dos últimos N dias)
    start_dt = last_dt - timedelta(days=window_days)
    start_str = start_dt.strftime("%Y-%m-%d")

    # =========================
    # Query incremental
    # =========================
    query = f"SELECT * FROM Data.Sales WHERE OrderDate >= '{start_str}'"

    with pyodbc.connect(conn_str) as conn:
        df_new = pd.read_sql(query, conn)

    if df_new.empty:
        print(f"OK (incremental): no rows since {start_str}. Nothing to do.")
        return 0

    # =========================
    # Escrita particionada por DIA (OrderDate)
    # =========================
    # Ao invés de reescrever um arquivo gigante (Data_Sales.parquet),
    # Escrever por partição:
    # landing/Data_Sales/OrderDate=YYYY-MM-DD/data.parquet

    sales_root = landing_dir / "Data_Sales"
    sales_root.mkdir(parents=True, exist_ok=True)

    # Garantir que OrderDate é date (não string)
    df_new["OrderDate"] = pd.to_datetime(df_new["OrderDate"]).dt.date

    # Para cada dia presente no lote incremental
    for order_date, df_day_new in df_new.groupby("OrderDate"):
        part_dir = sales_root / f"OrderDate={order_date.isoformat()}"
        part_dir.mkdir(parents=True, exist_ok=True)

        part_file = part_dir / "data.parquet"

        # Merge + dedupe dentro da partição do dia
        if part_file.exists():
            df_day_old = pd.read_parquet(part_file)
            df_day_all = pd.concat([df_day_old, df_day_new], ignore_index=True)
        else:
            df_day_all = df_day_new

        # PK composta da Sales: (OrderKey, LineNumber)
        df_day_all = df_day_all.drop_duplicates(subset=["OrderKey", "LineNumber"], keep="last")

        # Escrita atômica
        _atomic_write_parquet(df_day_all, part_file)

    # =========================
    # Atualizar watermark
    # =========================
    max_order_date = pd.to_datetime(df_new["OrderDate"]).max().date()

    _save_state(state_path, {
        "last_order_date": max_order_date.strftime("%Y-%m-%d")
    })

    print(
        f"OK (incremental/partitioned): root={sales_root} "
        f"rows_in_window={len(df_new)} watermark={max_order_date} window_start={start_str}"
    )

    return 0

# =========================
if __name__ == "__main__":
    raise SystemExit(main())