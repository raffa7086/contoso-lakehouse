import os #Acessar variáveis de ambiente
import sys #Acessar argumentos de linha de comando
from pathlib import Path #Resolver paths de forma independente do sistema operacional

import pandas as pd #Manipulação de DataFrames e escrita em Parquet
import pyodbc #Conexão com SQL Server
from dotenv import load_dotenv #Carregar variáveis de ambiente do arquivo .env


def main() -> int:
    # 1) Validar argumentos ANTES de qualquer coisa
    if len(sys.argv) != 2:
        print("Usage: python src/extract_test.py [schema].[table]")
        print("Example: python src/extract_test.py [Data].[Product]")
        return 1

    table = sys.argv[1]

    # 2) Resolver paths do projeto (independente de onde roda)
    base_dir = Path(__file__).resolve().parent.parent
    landing_dir = base_dir / "landing"
    landing_dir.mkdir(parents=True, exist_ok=True)

    # 3) Carregar credenciais do .env
    load_dotenv(base_dir / ".env")
    password = os.getenv("MSSQL_INGEST_PASSWORD")
    if not password:
        print("ERROR: MSSQL_INGEST_PASSWORD não encontrado no .env")
        return 2

    # 4) Conectar no SQL Server
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=ContosoV2_1M;"
        "UID=ingest_ro;"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )

    with pyodbc.connect(conn_str) as conn:
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, conn)

    # 5) Nome de arquivo determinístico a partir do nome da tabela
    output_file = table.replace("[", "").replace("]", "").replace(".", "_") + ".parquet"
    output_path = landing_dir / output_file

    df.to_parquet(output_path, index=False)
    print(f"OK: {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())