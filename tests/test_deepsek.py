import requests
import json

# Test mínimo que llama al mock en /tests si se levanta localmente.

payload = {
    "messages": [
        {"role": "user", "content": json.dumps({"schema": {"Country": "text"}, "question": "Dame un resumen"}, ensure_ascii=False)}
    ]
}

resp = requests.post("http://localhost:8080/", json=payload)
print(resp.status_code)
print(resp.text)
