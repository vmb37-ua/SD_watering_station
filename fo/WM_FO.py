import argparse, json, os, threading, time, uuid
from collections import deque
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from confluent_kafka import Producer, Consumer

# Topics de kafka
T_REQUESTS = "watering.requests"
T_NOTIFICATIONS = "watering.notifications"
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
ESPERA_ENTRE_SERVICIOS = 4

# TTL a central
TIMEOUT_RESPUESTA = 30 

# Al asignar las particiones nos ponemos a la cola
def alFinal(consumidor, particiones):
    for p in particiones:
        p.offset = consumidor.get_watermark_offsets(p, timeout=10)[1]
    consumidor.assign(particiones)

class Operario:
    def __init__(self, broker, operario):
        self.broker = broker
        self.operario = operario
        self.lock = threading.Lock()
        self.estaciones = []
        self.mensajes = deque(maxlen=100)
        self.peticiones = {}
        self.finalizadas = {}
        self.producer = Producer({"bootstrap.servers": broker})

    def log(self, texto):
        linea = f"{datetime.now().strftime('%H:%M:%S')} {texto}"
        print(linea, flush=True)
        with self.lock:
            self.mensajes.append(linea)

    def iniciar(self):
        asignado = threading.Event()
        consumer = Consumer({"bootstrap.servers":self.broker, "group.id" :f"fo-{self.operario}-{uuid.uuid4().hex[:6]}", "auto.offset.reset":"latest"})
        
        # Se suscribe al topic y pone el cursor al final. Lanza el evento asignado cuando esté listo
        consumer.subscribe([T_NOTIFICATIONS], on_assign=lambda c, p: (alFinal(c, p), asignado.set()))
        threading.Thread(target=self.bucle_consumo, args=(consumer,), daemon=True).start()
        self.log(f"Operario {self.operario} conectándose a Kafka en {self.broker}...")

        # Si llega el evento en 30s
        if asignado.wait(30):
            self.log("Conectado a Kafka")
        else:
            self.log("AVISO: Kafka aún no responde, seguimos intentándolo")


    def bucle_consumo(self, consumer):
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                m = json.loads(msg.value())
            except ValueError:
                continue
            if m.get("type") == "SNAPSHOT":
                with self.lock:
                    self.estaciones = m["stations"]
                continue
            # No se leen mensajes dirigidos a otros operarios
            if m.get("operator_id") != self.operario:
                continue 
            self.procesar(m)

    def procesar(self, m):
        peticion, tipo = m.get("request_id"), m.get("type")
        with self.lock:
            # Setdefault mete la clave al diccionario si no existía y la devuelve 
            p = self.peticiones.setdefault(peticion, {"request_id": peticion, "ws_id": m.get("ws_id"), "estado":"ENVIADA", "caudal": 0, "volumen": 0, "transcurrido": 0, "duracion": 0, "resumen": ""})
            if tipo == "TELEMETRY":
                p.update(estado="REGANDO", caudal=m["flow_lpm"], volumen=m["volume_l"], transcurrido=m["elapsed_s"])
            elif tipo == "AUTHORIZED":
                p.update(estado="REGANDO", duracion=m.get("duration_s", 0))
            elif tipo == "DENIED":
                p.update(estado="DENEGADA", resumen=m["msg"])
            elif tipo == "SUMMARY":
                p.update(estado="FINALIZADA", volumen=m.get("volume_l", 0), transcurrido=m.get("duration_s", 0), resumen=m["msg"])
        if tipo == "TELEMETRY":
            # Mensaje de log de la telemetría del riego
            print(f"{m['ws_id']}: caudal {m['flow_lpm']:.1f} L/min | volumen {m['volume_l']:.1f} L | {m['elapsed_s']} s", flush=True)
        else:
            self.log(f"[{m.get('ws_id')}] {m.get('msg')}")
        # El modo fichero verá el evento levantado y sabrá que esta tarea ha acabado
        if tipo in ("DENIED", "SUMMARY") and peticion in self.finalizadas:
            self.finalizadas[peticion].set()

    def solicitar(self, estacion, duracion):
        peticion = uuid.uuid4().hex[:8]
        self.finalizadas[peticion] = threading.Event()
        with self.lock: 
            self.peticiones[peticion] = {"request_id": peticion, "ws_id": estacion, "estado": "ENVIADA", "caudal": 0, "volumen": 0,"transcurrido": 0, "duracion": duracion, "resumen": ""}
        self.producer.produce(T_REQUESTS, json.dumps({"request_id": peticion, "operator_id": self.operario, "ws_id": estacion, "duration_s": duracion, "source": "FO"}).encode())
        self.producer.flush(5)
        self.log(f"Petición {peticion} enviada: regar {estacion} durante {duracion}s")
        return peticion

    # ================= (codigo de claude code para parsear el fichero) Modo fichero =================
    def procesar_fichero(self, ruta):
        # Aquí claude parsea el fichero para hacer la sintaxis de saltar comentarios, etc
        with open(ruta, encoding="utf-8") as f:
            servicios = [l.split("#")[0].strip() for l in f]
        servicios = [s for s in servicios if s]
        self.log(f"Fichero {ruta}: {len(servicios)} servicios a solicitar")
        for i, linea in enumerate(servicios, 1):
            partes = [x.strip() for x in linea.split(",")]
            ws_id = partes[0]
            duracion = int(partes[1]) if len(partes) > 1 and partes[1].isdigit() else 30
            self.log(f"--- Servicio {i}/{len(servicios)} ---")

            # Aquí mismo lanza la solicitud de riego automáticamente
            rid = self.solicitar(ws_id, duracion)
            
            # Esperamos a que termine (éxito o fracaso). Si nunca llega respuesta, no nos quedamos colgados
            limite = time.time() + TIMEOUT_RESPUESTA
            while not self.finalizadas[rid].wait(1):
                with self.lock:
                    estado = self.peticiones[rid]["estado"]
                if estado == "ENVIADA" and time.time() > limite:
                    self.log(f"CENTRAL no ha respondido a la petición {rid}. Se pasa a la siguiente")
                    break
            time.sleep(ESPERA_ENTRE_SERVICIOS)
        self.log("Fichero de servicios completado")

        # (Código de claude code para el panel) ================= Vista para la web =================
    def vista(self):
        with self.lock:
            return {"operador": self.operario, "estaciones": self.estaciones,
                    "peticiones": list(self.peticiones.values())[::-1][:20],
                    "mensajes": list(self.mensajes)[::-1]}


def arrancar_web(app, puerto):
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
                self._responder(200, app.vista())
            else:
                self._responder(404, {"error": "no encontrado"})

        def do_POST(self):
            if self.path != "/api/solicitar":
                return self._responder(404, {"error": "no encontrado"})
            try:
                c = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                rid = app.solicitar(str(c["ws_id"]).strip(), int(c.get("duracion", 30)))
                self._responder(200, {"request_id": rid})
            except (ValueError, KeyError) as e:
                self._responder(400, {"error": f"Petición inválida: {e}"})

        def log_message(self, *args):
            pass

    servidor = ThreadingHTTPServer(("0.0.0.0", puerto), Manejador)
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    app.log(f"Interfaz web en http://localhost:{puerto}")

def main():
    p = argparse.ArgumentParser(prog="WM_FO")
    p.add_argument("broker", help="IP:puerto del bootstrap-server de Kafka")
    p.add_argument("operador", help="ID del operario dado de alta en CENTRAL")
    p.add_argument("--fichero", help="Fichero con los servicios a solicitar")
    p.add_argument("--http-port", type=int, default=8000, help="Puerto de la interfaz web")
    a = p.parse_args()

    operario = Operario(a.broker, a.operador)
    arrancar_web(operario, a.http_port)
    operario.iniciar()
    if a.fichero:
        operario.procesar_fichero(a.fichero)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
