import sys


def _check_import(label: str, import_func) -> None:
    try:
        import_func()
        print(f"[OK] {label}")
    except Exception as exc:
        print(f"[ERROR] {label}: {exc}")


print("=" * 60)
print("ALLNALYZER - Diagnostic Report")
print("=" * 60)
print(f"Python: {sys.version}")
print(f"Executable: {sys.executable}")

print("\n" + "=" * 60)
print("Testing imports...")
print("=" * 60)

_check_import("src.core.parser", lambda: __import__("src.core.parser"))
_check_import("src.core.engine", lambda: __import__("src.core.engine"))
_check_import("src.database.config", lambda: __import__("src.database.config"))
_check_import("src.api.routes", lambda: __import__("src.api.routes"))
_check_import("src.utils.validators", lambda: __import__("src.utils.validators"))

print("\n" + "=" * 60)
print("All import checks finished. Starting Flask...")
print("=" * 60 + "\n")


if __name__ == "__main__":
    from web_app import app

    print("Flask app loaded successfully.")
    print("Server will start on http://localhost:5000")
    print("Press Ctrl+C to stop\n")
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
