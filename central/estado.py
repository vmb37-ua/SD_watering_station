# Este fichero maneja el modelo de datos de las estaciones y los distintos estados en los que está
# Tiene sentido para almacenar en memoria principal las cosas que no son persistentes o no es necesario que lo sean

import sqlite3, os
from datetime import datetime


DESCONECTADA = "DESCONECTADA"
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA_SERVICIO"

# Aqui van los operarios iniciales TODO

class Estacion:
    def __init__(self, id, ubicacion, bloqueada):
        self.id = id
        self.ubicacion = ubicacion
        self.bloqueada = bloqueada     
        self.conexion = None
        self.averia = None
        # Sesión para si esta regando, etc.
        self.sesion = None

    # Va a devolver el estado actual de la estación según sus atributos
    def estado(self):
        if self.conexion is None:
            return DESCONECTADA
        if self.averia:
            return FUGA
        if self.bloqueada:
            return FUERA_SERVICIO
        if self.sesion and self.sesion["estado"] == "ACTIVO":
            return REGANDO
        return DISPONIBLE

    # Según Claude, para enviar en JSON, debemos "empaquetar" el objeto en un diccionario
    def convertir_a_diccionario(self):
        dicc = {"id":self.id, "ubicacion":self.ubicacion, "estado":self.estado(), "averia":self.averia, "caudal":0, "volumen":0, "operador":None, "transcurrido":0, "duracion": 0}
        if self.sesion and self.sesion["estado"] == "ACTIVO":
            dicc["caudal"] = self.sesion["caudal"]
            dicc["volumen"] = self.sesion["volumen"]
            dicc["operador"] = self.sesion["operator_id"]
            dicc["transcurrido"] = self.sesion["transcurrido"]
            dicc["duracion"] = self.sesion["duracion"]
        return dicc

class Estado:
    def __init__(self, db_path):
        self.db_path = db_path
        self.estaciones = {}
        self.mensajes = [] 

        # Codigo de Claude para acceder a la bd
        if os.path.dirname(db_path):
            os.makedirs(os.path.dirname(db_path), exist_ok=True) 

        db = sqlite3.connect(self.db_path)
        db.execute('''CREATE TABLE IF NOT EXISTS operators(id TEXT PRIMARY KEY, name NOT NULL, active INTEGER NOT NULL DEFAULT 1);''')

        db.execute('''CREATE TABLE IF NOT EXISTS stations (id TEXT PRIMARY KEY, ubicacion TEXT, status TEXT NOT NULL,
        blocked INTEGER NOT NULL DEFAULT 0, ultimo_registro TEXT);''')
        # Las BD creadas con versiones anteriores no tienen la columna ubicacion
        columnas = [fila[1] for fila in db.execute("PRAGMA table_info(stations)").fetchall()]
        if "ubicacion" not in columnas:
            db.execute("ALTER TABLE stations ADD COLUMN ubicacion TEXT")

        db.execute('''CREATE TABLE IF NOT EXISTS logs_riego (id INTEGER PRIMARY KEY AUTOINCREMENT, ws_id TEXT NOT NULL REFERENCES stations(id),
        operator_id TEXT REFERENCES operators(id), started_at TEXT, ended_at TEXT, volumen_l REAL, salida TEXT);''')

        # Operarios que se crean al arrancar
        if db.execute("SELECT COUNT(*) FROM operators").fetchone()[0] == 0:
            db.execute("INSERT INTO operators (id, name) VALUES ('OP1', 'Operario 1')")
            db.execute("INSERT INTO operators (id, name) VALUES ('OP2', 'Operario 2')")
            db.execute("INSERT INTO operators (id, name) VALUES ('OP3', 'Operario 3')")
            db.execute("INSERT INTO operators (id, name) VALUES ('OP4', 'Operario 4')")
            # Las peticiones del panel de central y de las estaciones de riego las vamos a tratar como si fuesen operarios
            db.execute("INSERT INTO operators (id, name) VALUES ('LOCAL', 'Menu de la propia estacion')")
            db.execute("INSERT INTO operators (id, name) VALUES ('CENTRAL', 'Ordenes de la central')")

        for id, ubicacion, blocked in db.execute("SELECT id, ubicacion, blocked FROM stations").fetchall():
            self.estaciones[id] = Estacion(id, ubicacion, blocked == 1)
        db.execute("UPDATE stations SET status = ?", (DESCONECTADA,))
        db.commit()
        db.close()
        self.log(f"Se han cargado {len(self.estaciones)} estaciones desde la BD")

    def log(self, texto):
        linea = datetime.now().strftime('%H:%M:%S') + " " + texto
        print("[LOG]", linea)
        self.mensajes.append(linea)
        # Vamos a almacenar solo los ultimos 200 logs aqui
        if len(self.mensajes) > 200:
            self.mensajes.pop(0)

    def registrar(self, id, ubicacion):
        db = sqlite3.connect(self.db_path)
        if id not in self.estaciones:
            db.execute("INSERT INTO stations (id, ubicacion, status, blocked, ultimo_registro) VALUES (?,?,?,?,?)",(id, ubicacion, DESCONECTADA, 0, datetime.now().isoformat()))
            self.estaciones[id] = Estacion(id, ubicacion, False)
            self.log(f"+ Alta de nueva estación: {id} -> [{ubicacion}]")
        else:
            # Por si se registra cun un id ya conocido
            db.execute("UPDATE stations SET ubicacion = ? WHERE id = ?", (ubicacion, id))
            self.estaciones[id].ubicacion = ubicacion
        db.commit()
        db.close()

    # Guarda en la BD el estado de la estacion id
    def persistir(self, id):
        estacion = self.estaciones[id]
        db = sqlite3.connect(self.db_path)
        db.execute("UPDATE stations SET status = ?, blocked = ?, ultimo_registro = ? WHERE id = ?", (estacion.estado(), int(estacion.bloqueada), datetime.now().isoformat(), id))
        db.commit()
        db.close()

    def guardar_riego(self, id, operador, inicio, fin, volumen, salida):
        db = sqlite3.connect(self.db_path)
        db.execute("INSERT INTO logs_riego (ws_id, operator_id, started_at, ended_at, volumen_l, salida) VALUES (?,?,?,?,?,?)",(id, operador, inicio, fin, volumen, salida))
        db.commit()
        db.close()

# Logica de los operarios

    def operario_valido(self, id):
        # Devuelve true si existe el id de operario y esta activo
        db = sqlite3.connect(self.db_path)
        fila = db.execute("SELECT active FROM operators WHERE id = ?", (id,)).fetchone()
        db.close()
        return fila is not None and fila[0] == 1

    def operarios(self):
        db = sqlite3.connect(self.db_path)
        filas = db.execute("SELECT id, name FROM operators ORDER BY id").fetchall()
        lista = []
        for id, nombre in filas:
            lista.append({"id":id, "nombre":nombre})
        db.close()
        return lista

    def alta_operario(self, id, nombre):
        db = sqlite3.connect(self.db_path)
        db.execute("INSERT OR REPLACE INTO operators (id, name, active) VALUES (?,?,1)", (id, nombre))
        db.commit()
        db.close()
        self.log(f"+ Alta del operario {id} -> ({nombre})")

    def instantanea(self):
        # una lista de todas las estaciones como diccionarios
        lista = []
        for id in sorted(self.estaciones):
            lista.append(self.estaciones[id].convertir_a_diccionario())
        return lista