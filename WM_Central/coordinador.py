import json, threading, time, uuid
from datetime import datetime
# Librerias para manejar kafka desde python
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from estado import DISPONIBLE

# Lista de topics de kafka
T_REQUESTS = "watering.requests" # peticion de riego
T_COMMANDS = "watering.commands" # ordenes de central a estacion
T_TELEMETRY = "watering.telemetry" # caudal y vloumen
T_EVENTS = "watering.events" # respuesta de estacion a central
T_NOTIFICATIONS = "watering.notifications" # comunicacion de los pasos al operario, resumen
TOPICS = [T_REQUESTS, T_COMMANDS, T_TELEMETRY, T_EVENTS, T_NOTIFICATIONS]

TIMEOUT_AUTORIZACION = 5 # segundos que se esperan a que la estacion acepte inicio
PERIODO_LISTA = 2 # cada cuanto se envia la lista de estaciones a los operarios

MOTIVOS = {
    "TIMEOUT": "tiempo límite alcanzado",
    "MANUAL": "finalizado desde el menú de la estación",
    "LEAK": "fuga o avería detectada",
    "BLOCKED": "estación bloqueada por CENTRAL",
    "CENTRAL": "parado por CENTRAL",
    "DESCONEXION": "estación desconectada de CENTRAL",
    "REINICIO": "el engine de la estación se ha reiniciado",
}

def alFinal(consumer, particiones):
    # Las particiones de kafka solo las puede leer uno del mismo grupo, y nos interesa que sea en el ultimo offset
    # Esta funcion coloca al consumidor en el ultimo offset de las particiones para que lea inmediatamente los mensajes mas recientes
    for p in particiones:
        p.offset = consumer.get_watermark_offsets(p, timeout=10)[1]
    consumer.assign(particiones)

class Coordinador:
    def __init__(self, estado, broker):
        self.estado = estado
        self.broker = broker
        self.producer = Producer({"bootstrap.servers": broker})

    def arrancar(self):
        threading.Thread(target=self._bucle_consumo, daemon=True).start()
        threading.Thread(target=self._bucle_periodico, daemon=True).start()

    # Esta funcion crea los topics si no existen, solo con 1 particion
    def _crear_topics(self):
        admin = AdminClient({"bootstrap.servers": self.broker})
        while True:
            try:
                existentes = admin.list_topics(timeout=5).topics
                nuevos = [NewTopic(t, num_partitions=1, replication_factor=1) for t in TOPICS if t not in existentes]
                for t, futuro in admin.create_topics(nuevos).items() if nuevos else []:
                    try:
                        futuro.result()
                        self.estado.log(f"Topic {t} creado")
                    except Exception as e:
                        self.estado.log(f"No se pudo crear el topic {t}: {e}")
                return
            except KafkaException as e:
                self.estado.log(f"Kafka ({self.broker}) no disponible, reintentando... ({e})")
                time.sleep(3)

    def enviar(self, topic, mensaje):
        # Publicamos al topic en formato json. El encode es para que cuadre la codificación de texto utf8
        self.producer.produce(topic, json.dumps(mensaje).encode())
        # poll quita tras x segundos el reporte del envío de la cola de la libreria. Si se acumularan se llenaria ese buffer
        self.producer.poll(0)

    # El operador ** es para hacer funciones poliadicas, recibo cualquier numero de argumentos nombrados.
    def comando(self, id, tipo, **cosas):
        self.enviar(T_COMMANDS, {"type": tipo, "ws_id": id, **cosas})

    def notificar(self, operador, peticion, estacion, tipo, texto, **cosas):
        self.estado.log(f"[OP {operador} -> EST {estacion}] {texto}")
        self.enviar(T_NOTIFICATIONS, {"type": tipo, "operator_id":operador, "request_id":peticion, "ws_id":estacion, "msg":texto, "ts": time.time(), **cosas})

    def _bucle_consumo(self):
        self._crear_topics()
        # Creamos el consumidor con un grupo único cada vez (en el mismo grupo dos instancias no podrian leer la misma partición) y suscribimos a los topics que vamos a leer
        consumer = Consumer({"bootstrap.servers": self.broker, "group.id": f"wm-central-{uuid.uuid4().hex[:8]}","auto.offset.reset": "latest"})
        consumer.subscribe([T_REQUESTS, T_EVENTS, T_TELEMETRY], on_assign=alFinal)
        self.estado.log(f"Conectado a Kafka en {self.broker}")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                self.estado.log(f"Error de Kafka: {msg.error()}")
                continue
            try:
                datos = json.loads(msg.value())
                if msg.topic() == T_REQUESTS:
                    self.peticion(datos)
                elif msg.topic() == T_EVENTS:
                    self.evento(datos)
                elif msg.topic() == T_TELEMETRY:
                    self.telemetria(datos)
            except Exception as e:
                self.estado.log(f"Mensaje inválido en {msg.topic()}: {e}")

    def _bucle_periodico(self):
        ultimo_snapshot = 0
        while True:
            time.sleep(1)
            ahora = time.time()
            # Recogemos las peticiones caducadas si existe una sesion (pendiente o activa) y ademas está pendiente y ya le tocaba regar
            caducadas = [e.id for e in self.estado.estaciones.values() if e.sesion and e.sesion["estado"] == "PENDIENTE" and e.sesion["limite"] < ahora]
            # Cerramos la sesión de las que no han contestado (aparentemente rotas)
            for ws in caducadas:
                self.cerrar_sesion(ws, "La estación no ha respondido a la solicitud de autorización")
            # Cada PERIODO_SANPSHOT segundos enviamos la lista de las estaciones a los operarios
            if ahora - ultimo_snapshot >= PERIODO_LISTA:
                ultimo_snapshot = ahora
                self.enviar(T_NOTIFICATIONS, {"type": "SNAPSHOT", "operator_id": "*", "stations": self.estado.instantanea()})

    def peticion(self, mensaje):
        # Nos aseguramos de que se pasen los parametros aunque alguno no exista. Si no hay id se genera un hexadecimal del UUID para esta instancia
        self.solicitar_riego(mensaje.get("operator_id", "?"), mensaje.get("ws_id", "?"), mensaje.get("duration_s", 30), mensaje.get("request_id") or uuid.uuid4().hex[:8])

    def validar(self, operario, estacion, duracion):
        # Devolvemos el motivo por el que no se permite el uso del operario sobre la estacion
        if not self.estado.operario_valido(operario):
            return f"El operario {operario} no está dado de alta en CENTRAL"
        if duracion <= 0 or duracion > 3600:
            return "Duración no válida (1-3600 s)"
        est = self.estado.estaciones.get(estacion)
        if est is None:
            return f"La estación {estacion} no existe"
        if est.estado() != DISPONIBLE:
            return f"La estacion {estacion} no está disponible (estado {est.estado()})"
        if est.sesion:
            return f"La estación {estacion} ya tiene un riego en curso o pendiente"
        return None

    def solicitar_riego(self, operario, estacion, tiempo, peticion):
        try:
            duracion = int(tiempo)
        except (TypeError, ValueError):
            duracion = -1
            
        self.notificar(operario, peticion, estacion, "INFO", f"Petición {peticion} recibida: regar {estacion} durante {duracion}s")  
        motivo = self.validar(operario, estacion, duracion)
        if motivo:
            self.notificar(operario, peticion, estacion, "DENIED", f"Riego denegado: {motivo}", reason=motivo)
            return

        # Creamos la sesion pendiente de la estacion en el modelo y enviamos el comando a la estacion
        self.estado.estaciones[estacion].sesion = {
            "request_id": peticion, "operator_id": operario, "estado": "PENDIENTE", "duracion": duracion,
            "limite": time.time() + TIMEOUT_AUTORIZACION, "inicio": None, "caudal": 0, "volumen": 0, "transcurrido": 0}
        self.notificar(operario, peticion, estacion, "INFO", f"Comprobaciones OK. Solicitando autorización a la estación {estacion}...")
        self.comando(estacion, "START", request_id=peticion, operator_id=operario, duration_s=duracion)

    # Devuelve la sesion de una estacion si esta corresponde a la que se pasa como argumento
    def sesion(self, estacion, peticion):
        est = self.estado.estaciones.get(estacion)
        if est and est.sesion and est.sesion["request_id"] == peticion:
            return est.sesion
        return None

    # Cierra y elimina la sesion de una estacion
    def cerrar_sesion(self, estacion, motivo, volumen=None, duracion=None):
        est = self.estado.estaciones.get(estacion)
        if est is None or est.sesion is None:
            return
        ses = est.sesion
        est.sesion = None
        # Si no devuelve nada enviamos el mismo motivo
        texto_motivo = MOTIVOS.get(motivo, motivo)
        if ses["estado"] == "PENDIENTE":
            self.notificar(ses["operator_id"], ses["request_id"], estacion, "DENIED", f"Riego DENEGADO: {texto_motivo}", reason=texto_motivo)
        else:
            # Si no conocemos volumen y duracion por interrupcion abrupta podemos usar los que se pasen por parametro
            volumen = ses["volumen"] if volumen is None else float(volumen)
            duracion = time.time() - ses["inicio"] if duracion is None else float(duracion)

            # Después guardamos en la base de datos como riego finalizado y notificamos al operario 
            self.estado.guardar_riego(estacion, ses["operator_id"], datetime.fromtimestamp(ses["inicio"]).isoformat(),datetime.now().isoformat(), volumen, motivo)
            self.notificar(ses["operator_id"], ses["request_id"], estacion, "SUMMARY", f"Riego FINALIZADO en {estacion} ({texto_motivo}). Volumen total: {volumen:.1f} L. Duración: {duracion:.0f} s",volume_l=round(volumen, 2), duration_s=round(duracion), reason=texto_motivo)
        self.estado.persistir(estacion)

    def evento(self, m):
        estacion, tipo, peticion = m.get("ws_id"), m.get("type"), m.get("request_id")
        est = self.estado.estaciones.get(estacion)
        if est is None:
            self.estado.log(f"Evento {tipo} sobre una estacion que no esxiste: {estacion}")
            return
        
        s = self.sesion(estacion, peticion)
        # Aqui manejamos los diferentes evntos que pueden llegar de la estacion y mantenemos la consistencia con el modelo y BD
        if tipo == "ONLINE":
            self.estado.log(f"Motor de {estacion} conectado a Kafka")
            # Si ya existia una sesion la reiniciamos y mantenemos su estado de bloqueo
            if est.sesion:
                self.cerrar_sesion(estacion, "REINICIO")
            self.comando(estacion, "STATE", blocked=est.bloqueada)
        elif tipo == "STARTED" and s:
            s["estado"] = "ACTIVO"
            s["inicio"] = time.time()
            self.estado.persistir(estacion)
            self.notificar(s["operator_id"], peticion, estacion, "AUTHORIZED", f"Riego AUTORIZADO. Electroválvula de {estacion} abierta ({s['duracion']} s)", duration_s=s["duracion"])
        elif tipo == "REJECTED" and s:
            self.cerrar_sesion(estacion, f"La estación ha rechazado el riego ({m.get('reason')})")
        elif tipo == "FINISHED" and s:
            self.cerrar_sesion(estacion, m.get("reason", "?"), m.get("volume_l"), m.get("elapsed_s"))
        else:
            self.estado.log(f"Evento {tipo} de {estacion} ignorado (petición {peticion} no activa)")

    # Actualizamos la telemetria del riego y la notificacion al operario
    def telemetria(self, m):
        estacion = m.get("ws_id")
        s = self.sesion(estacion, m.get("request_id"))
        if not s or s["estado"] != "ACTIVO":
            return
        s["caudal"] = m["flow_lpm"]
        s["volumen"] = m["volume_l"]
        s["transcurrido"] = m["elapsed_s"]
        operador = s["operator_id"]
        peticion = s["request_id"]
        self.enviar(T_NOTIFICATIONS, {"type":"TELEMETRY", "operator_id":operador, "request_id":peticion, "ws_id":estacion,"flow_lpm": m["flow_lpm"], "volume_l": m["volume_l"], "elapsed_s": m["elapsed_s"]})

    def conectar(self, estacion, token):
        est = self.estado.estaciones[estacion]
        # El token es el numero de conexion que genera el iterador en central
        est.conexion = token 
        est.averia = None
        self.estado.persistir(estacion)
        self.estado.log(f"Estación {estacion} autenticada y conectada -> {est.estado()}")
        # Si la central la bloquea mientras esta apagada se acutaliza
        self.comando(estacion, "STATE", blocked=est.bloqueada)

    def desconectar(self, estacion, token):
        est = self.estado.estaciones.get(estacion)
        # Si el token no coincide es que se ha reconectado por otra conexion de socket
        if est is None or est.conexion != token:
            return
        est.conexion = None
        if est.sesion:
            self.comando(estacion, "STOP")
            self.cerrar_sesion(estacion, "DESCONEXION")
        self.estado.persistir(estacion)
        self.estado.log(f"Estación {estacion} DESCONECTADA")

    def alerta(self, estacion, motivo):
        est = self.estado.estaciones[estacion]
        est.averia = motivo
        if est.sesion:
            self.comando(estacion, "STOP")
            self.cerrar_sesion(estacion, "LEAK")
        self.estado.persistir(estacion)
        self.estado.log(f"[!] ALERTA en {estacion}: {motivo} -> FUGA")

    def limpiar(self, estacion):
        est = self.estado.estaciones[estacion]
        est.averia = None
        self.estado.persistir(estacion)
        self.estado.log(f"Avería de {estacion} resuelta -> {est.estado()}")

    def orden(self, accion, destino, duracion=30):
        if destino == "*":
            destinos = list(self.estado.estaciones)  
        else:
            destinos = [destino]

        for id in destinos:
            est = self.estado.estaciones.get(id)
            if est is None:
                return f"La estación {id} no existe"
            if accion == "START":
                self.solicitar_riego("CENTRAL", id, duracion, uuid.uuid4().hex[:8])
            elif accion == "BLOCK":
                est.bloqueada = True
                self.comando(id, "BLOCK")
                if est.sesion:
                    self.cerrar_sesion(id, "BLOCKED")
                self.estado.persistir(id)
                self.estado.log(f"CENTRAL bloquea {id} -> FUERA DE SERVICIO")
            elif accion == "UNBLOCK":
                est.bloqueada = False
                self.comando(id, "UNBLOCK")
                self.estado.persistir(id)
                self.estado.log(f"CENTRAL desbloquea {id} -> {est.estado()}")
            else:
                return f"Acción {accion} desconocida"
        return None