#!/usr/bin/env python3
"""
serve.py — Servidor local de desarrollo
Sirve los JSONs de /data y un índice de fondos disponibles.
Uso: python serve.py
"""
import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
PORT = 3001


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # CORS para desarrollo
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Type", "application/json")

        if self.path == "/api/funds":
            # Índice de todos los fondos disponibles
            funds = []
            for json_file in sorted(DATA_DIR.glob("*.json")):
                with open(json_file) as f:
                    data = json.load(f)
                funds.append({
                    "isin":    data["meta"]["isin"],
                    "nombre":  data["meta"].get("nombre"),
                    "gestora": data["meta"].get("gestora"),
                    "estado":  data["meta"]["extraccion_estado"]
                })
            body = json.dumps(funds, ensure_ascii=False).encode()
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        elif self.path.startswith("/data/") and self.path.endswith(".json"):
            isin = self.path.replace("/data/", "").replace(".json", "")
            json_path = DATA_DIR / f"{isin}.json"
            if json_path.exists():
                with open(json_path, "rb") as f:
                    body = f.read()
                self.send_header("Content-Length", len(body))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        print(f"[serve] {args[0]} {args[1]}")


if __name__ == "__main__":
    print(f"[serve] Servidor de datos en http://localhost:{PORT}")
    print(f"[serve] Fondos disponibles: {len(list(DATA_DIR.glob('*.json')))}")
    HTTPServer(("localhost", PORT), Handler).serve_forever()
