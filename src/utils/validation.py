import re
from typing import List, Tuple


FORBIDDEN_SQL_TOKENS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "copy",
    "attach",
    "detach",
    "pragma",
}


def extract_quoted_identifiers(sql: str) -> List[str]:
    """Extrae identificadores envueltos en comillas dobles."""
    return re.findall(r'"([^"]+)"', sql)


def _strip_string_literals(sql: str) -> str:
    """Quita literales para no confundir textos con identificadores."""
    return re.sub(r"'([^']|'')*'", "''", sql)


def validate_sql_columns(sql: str, allowed_columns: List[str]) -> Tuple[bool, str]:
    """
    Paso 9:
    Validacion defensiva antes de entregar SQL a DuckDB.

    La regla es intencionalmente simple para el MVP:
    - solo SELECT;
    - una sola sentencia;
    - sin DDL/DML;
    - identificadores entre comillas deben existir en el esquema;
    - la consulta debe apuntar a la vista local `data` o `parquet_view`.

    Los alias no se bloquean porque el LLM puede generar `COUNT(*) AS total`.
    DuckDB hace la validacion final de columnas reales.
    """
    if not sql or not sql.strip():
        return False, "La consulta SQL esta vacia."

    cleaned = _strip_string_literals(sql).strip()
    cleaned_no_tail = cleaned.rstrip(";").strip()

    if ";" in cleaned_no_tail:
        return False, "Solo se permite una sentencia SQL."

    if not re.match(r"^\s*select\b", cleaned_no_tail, flags=re.IGNORECASE):
        return False, "Solo se permiten consultas SELECT."

    tokens = {token.lower() for token in re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", cleaned)}
    forbidden = sorted(tokens.intersection(FORBIDDEN_SQL_TOKENS))
    if forbidden:
        return False, f"Token SQL no permitido: {forbidden[0]}"

    if not re.search(r"\bfrom\s+(data|parquet_view)\b", cleaned, flags=re.IGNORECASE):
        return False, "La consulta debe leer desde la vista `data`."

    allowed_lower = {column.lower() for column in allowed_columns}
    for identifier in extract_quoted_identifiers(cleaned):
        if identifier.lower() not in allowed_lower:
            return False, f"Identificador entre comillas no permitido: {identifier}"

    return True, "OK"
