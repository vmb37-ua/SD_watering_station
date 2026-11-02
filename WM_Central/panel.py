import json, os, threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


def arrancar_panel(coord, puerto):
    '''Panel web de monitorización y control de CENTRAL (front web, figura 2 del enunciado)'''

    class Manejador(BaseHTTPRequestHandler):
        def _responder(self, codigo, cuerpo, tipo="application/json"):
            datos = cuerpo if isinstance(cuerpo, bytes) else json.dumps(cuerpo).encode()
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(datos)))
            self.end_headers()
            self.wfile.write(datos)

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                with open(os.path.join(STATIC, "index.html"), "rb") as f:
                    self._responder(200, f.read(), "text/html; charset=utf-8")
            elif self.path == "/api/estado":
                self._responder(200, {"estaciones": coord.estado.instantanea(),
                                      "eventos": list(coord.estado.mensajes)[-60:][::-1],
                                      "operadores": coord.estado.operarios()})
            else:
                self._responder(404, {"error": "no encontrado"})

        def do_POST(self):
            try:
                cuerpo = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            except ValueError:
                return self._responder(400, {"error": "JSON inválido"})
            if self.path == "/api/orden":
                error = coord.orden(cuerpo.get("accion"), cuerpo.get("ws_id"), cuerpo.get("duracion", 30))
            elif self.path == "/api/operarios":
                if not cuerpo.get("id"):
                    error = "Falta el id del operario"
                else:
                    coord.estado.alta_operario(cuerpo["id"].strip(), cuerpo.get("nombre", "").strip() or cuerpo["id"])
                    error = None
            else:
                return self._responder(404, {"error": "no encontrado"})
            self._responder(400 if error else 200, {"error": error} if error else {"ok": True})

        def log_message(self, *args):
            pass  # Sin log de cada petición HTTP, el panel consulta cada segundo

    servidor = ThreadingHTTPServer(("0.0.0.0", puerto), Manejador)
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    coord.estado.log(f"Panel web escuchando en http://0.0.0.0:{puerto}")
