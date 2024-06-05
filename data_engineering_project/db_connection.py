import duckdb


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("central-hmni.duckdb", read_only=False)

def get_readonly_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("central-hmni.duckdb", read_only=True)
