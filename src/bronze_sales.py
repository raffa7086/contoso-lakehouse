import json
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# ============================================================
# Bronze (Sales) - Incremental por janela (30 dias)
# ============================================================
# Objetivo:
#   - Ler o RAW append-only (landing/raw/Data_Sales/OrderDate=.../batch=.../*.parquet)
#   - Para cada dia (partição OrderDate) dentro da janela:
#       1) Ler TODOS os arquivos daquele dia (inclui múltiplos batches)
#       2) Deduplicar por PK (OrderKey, LineNumber) escolhendo o batch mais recente
#       3) Escrever Bronze particionado por dia:
#          bronze/Data_Sales/OrderDate=YYYY-MM-DD/data.parquet


# -------------------------
# Funções utilitárias: estado (JSON)
# -------------------------
def load_state(path: Path) -> dict:
    """
    Lê um arquivo JSON de estado.
    Se o arquivo não existir, retorna {}.
    """
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save_state(path: Path, state: dict) -> None:
    """
    Salva um JSON de estado.
    Garante que o diretório exista antes de escrever.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


# -------------------------
# Funções utilitárias: detectar timestamp do batch
# -------------------------
# Esperamos algo assim no caminho:
#   .../batch=2026-03-01T17-46-00Z/...
BATCH_TS_RE = re.compile(r"batch=(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)")


def parse_batch_ts_from_path(p: Path) -> datetime:
    """
    Extrai o timestamp do batch a partir do caminho do arquivo parquet.

    Se encontrar 'batch=YYYY-MM-DDTHH-MM-SSZ', converte para datetime (UTC).
    Se não encontrar (caso seu layout não tenha batch=...), usa mtime do arquivo
    como fallback (menos robusto, mas evita quebrar).
    """
    m = BATCH_TS_RE.search(str(p).replace("\\", "/"))
    if m:
        raw = m.group(1)  # ex: 2026-03-01T17-46-00Z
        return datetime.strptime(raw, "%Y-%m-%dT%H-%M-%SZ")

    # data de modificação do arquivo no disco
    return datetime.utcfromtimestamp(p.stat().st_mtime)


# -------------------------
# Pipeline Bronze
# -------------------------
def main() -> int:
    # 1) Diretórios base do projeto
    base_dir = Path(__file__).resolve().parent.parent

    # RAW (entrada)
    # Esperado: landing/raw/Data_Sales/OrderDate=YYYY-MM-DD/batch=.../*.parquet
    raw_root = base_dir / "landing" / "raw" / "Data_Sales"

    # BRONZE (saída)
    bronze_root = base_dir / "bronze" / "Data_Sales"

    # Estado do RAW (watermark)
    # Deve conter: {"last_order_date": "YYYY-MM-DD"}
    state_raw_path = base_dir / "state" / "Data_Sales_raw.json"

    # Estado do Bronze
    # {"last_processed_order_date": "YYYY-MM-DD"}
    state_bronze_path = base_dir / "state" / "Data_Sales_bronze.json"

    # Validação: RAW existe?
    if not raw_root.exists():
        print(f"ERRO: caminho RAW não encontrado: {raw_root}")
        return 1

    # 2) Ler watermark do RAW (última OrderDate carregada no RAW)
    state_raw = load_state(state_raw_path)
    watermark_str = state_raw.get("last_order_date")

    if not watermark_str:
        print(f"ERRO: estado RAW sem 'last_order_date' em: {state_raw_path}")
        return 2

    watermark_dt = datetime.strptime(watermark_str, "%Y-%m-%d").date()

    # 3) Definir janela incremental (30 dias)
    window_days = 30
    window_start = watermark_dt - timedelta(days=window_days)

    print(f"INFO: watermark={watermark_dt} window_start={window_start} (janela={window_days}d)")

    # 4) Descobrir quais partições (dias) existem no RAW dentro da janela
    # Pastas no formato: OrderDate=YYYY-MM-DD
    day_dirs: list[tuple[datetime.date, Path]] = []

    for d in raw_root.glob("OrderDate=*"):
        if not d.is_dir():
            continue

        day_str = d.name.split("=", 1)[1]
        try:
            day_dt = datetime.strptime(day_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        # Filtra pela janela
        if window_start <= day_dt <= watermark_dt:
            day_dirs.append((day_dt, d))

    if not day_dirs:
        print("OK: nenhuma partição RAW encontrada na janela. Nada a fazer.")
        return 0

    # Ordena por data (mais antigo -> mais novo)
    day_dirs.sort(key=lambda x: x[0])

    # 5) Para cada dia: ler todos os batches, deduplicar e escrever Bronze daquele dia
    total_days = 0
    total_rows_in = 0
    total_rows_out = 0

    for day_dt, day_path in day_dirs:
        # Procura parquets em qualquer nível abaixo do dia (inclui batch=...)
        files = list(day_path.rglob("*.parquet"))
        if not files:
            continue

        # Ler todos os parquets do dia e “taggear” cada linha com timestamp do batch
        dfs = []
        for f in files:
            df = pd.read_parquet(f)

            # Essa coluna serve APENAS para escolher qual linha vence no dedupe.
            # Regra: “batch mais recente ganha”.
            batch_ts = parse_batch_ts_from_path(f)
            df["_ingest_batch_ts_utc"] = batch_ts

            dfs.append(df)

        df_all = pd.concat(dfs, ignore_index=True)
        total_rows_in += len(df_all)

        # 5.2) Deduplicar por PK composta (OrderKey, LineNumber)
        df_all = df_all.sort_values(["OrderKey", "LineNumber", "_ingest_batch_ts_utc"])
        df_out = df_all.drop_duplicates(subset=["OrderKey", "LineNumber"], keep="last")

        # Remove a coluna auxiliar antes de gravar Bronze (não é dado de negócio)
        df_out = df_out.drop(columns=["_ingest_batch_ts_utc"])

        # Escrever Bronze da partição do dia (overwrite do dia)
        out_dir = bronze_root / f"OrderDate={day_dt.strftime('%Y-%m-%d')}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / "data.parquet"
        df_out.to_parquet(out_file, index=False)

        total_rows_out += len(df_out)
        total_days += 1

        print(f"OK: bronze dia={day_dt} linhas_in={len(df_all)} linhas_out={len(df_out)} -> {out_file}")

    # 6) Salvar estado do Bronze
    # Guardando o que o Bronze processou até o watermark atual do RAW.
    save_state(state_bronze_path, {"last_processed_order_date": watermark_dt.strftime("%Y-%m-%d")})

    print(
        f"DONE: dias_processados={total_days} "
        f"rows_in={total_rows_in} rows_out={total_rows_out} "
        f"bronze_root={bronze_root}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())