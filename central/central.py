import socket, sqlite3, threading, os, argparse, itertools

from estado import Estado
from datetime import datetime

IP = '0.0.0.0' # Es necesario escuchar en todas las interfaces de red porque no sabemos cual asigna docker al exterior
PORT = int(os.getenv('PORT', 5000))
DB_PATH = os.getenv('DB_PATH', "./database.db")

STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

class ManejadorDatos():
    def __init__(self, comando="", args=[], respuesta = ""):
        self.comando = comando
        self.args = args
        self.respuesta = respuesta
    
    def leerDatos(self, datos:str):

        # Naturalmente el caracter # no se puede utilizar como un dato más, es el separador
        lista = datos.split("#")
        self.comando = lista[0]
        self.args = lista[1:]

        print(f"[LOG] Central recibido --> {self.comando} - {self.args}")

        # Aqui vamos a separar los tipos de mensaje y vamos a llamar a su manejador
        # (Echo de menos aqui el switch de C)
        if self.comando == "AUTH":
            self.respuesta = self.manejar_AUTH()
        elif self.comando == "REG":
            self.respuesta = self.manejar_REG()
        elif self.comando == "ALERT":
            self.respuesta = self.manejar_ALERT()
        elif self.comando == "CLEAR":
            self.respuesta = self.manejar_CLEAR()
        elif self.comando == "BYE":
            self.respuesta = self.manejar_BYE()
        else:
            print("Comando no reconocido:", self.comando)

# TODO -> Acabar manejadores
    def manejar_AUTH(self):
        # AUTH#<estacion>
        conn = sqlite3.connect(DB_PATH)
        try:
            with conn:
                fila = conn.execute("SELECT * FROM stations WHERE id = ?", (self.args[0],)).fetchone()
                if fila is None:
                    conn.execute("INSERT INTO stations (id, status, blocked, ultimo_registro) VALUES (?,?,?,?)", (self.args[0], "DISPONIBLE", 0, datetime.now().isoformat()))
                else:
                    conn.execute("UPDATE stations SET status = ? WHERE id = ?", ("DISPONIBLE", self.args[0]))
        except Exception as e:
            return f"ERR_AUTH#Error de Sqlite -> {e}"
            
        finally:
            conn.close()

        return f"OK_AUTH#{self.args[0]}"
        
    def manejar_REG(self):
        # REG#<estacion>#<volumen>#<caudal>
        ...
    def manejar_ALERT(self):
        # ALERT#<estacion>#<motivo: LEAK|(otros)>
        estacion = self.args[0]
        motivo = self.args[1]
        
        if motivo == "LEAK":
            status = "FUGA"
        else:
            status = "FUERA_SERVICIO"
        
        conn = sqlite3.connect(DB_PATH)
        try:
            with conn:
                conn.execute("UPDATE stations SET status = ?, blocked = 1, ultimo_registro = ? WHERE id = ?", (status, datetime.now().isoformat(), estacion))
        except Exception as e:
            return f"ERR_ALERT#Error de sqlite -> {e}"
        finally:
            conn.close()

        return f"OK_ALERT#{estacion}"

    def manejar_CLEAR(self):
        # CLEAR#<estacion>
        estacion = self.args[0]
        conn = sqlite3.connect(DB_PATH)
        try:
            with conn:
                conn.execute("UPDATE stations SET status = ?, blocked = 0, ultimo_registro = ? WHERE id = ?", ("DISPONIBLE", datetime.now().isoformat(), estacion))
        except Exception as e:
            return f"ERR_CLEAR#{e}"
        finally:
            conn.close()

        return f"OK_CLEAR#{estacion}"

    def manejar_BYE(self):
        # BYE#<estacion>
        return f"OK_BYE#{self.args[0]}"
class OperadorTramas:
    '''Quizá es complicarlo demasiado, pero hacer esto y no un .find para separar las tramas
    es mucho más robusto ya que evitamos la agrupación y la fragmentación de trams al no conocer exactamente su tamaño'''
    def __init__(self, conn):
        self.conn = conn
        self.buffer = b''

    def comprobar_lrc(self, lrc, datos):
        # Como decisión de diseño vamos a tomar el LRC solo de la parte de los datos
        ac = 0
        for b in datos:
            ac ^= b
        return ac == lrc

    def siguiente(self):
        while True:
            i = self.buffer.find(STX)
            j = self.buffer.find(ETX, i+1) if i != -1 else -1
            if i != -1 and j != -1 and len(self.buffer) >= j + 2: # Comprobamos que existen y existe el byte LRC
                datos = bytes(self.buffer[i+1:j])
                lrc = self.buffer[j+1]
                self.buffer = self.buffer[j+2:]

                if self.comprobar_lrc(lrc, datos):
                    return datos.decode()
                else:
                    raise ValueError()
            trozo = self.conn.recv(4096)
            if not trozo:
                raise ConnectionError("El cliente ha cerrado")
            self.buffer += trozo

    def construir_trama(self, datos):
        lrc = 0
        for b in datos:
            lrc ^= b
        return STX + datos + ETX + bytes([lrc])

def atender(conn, addr):
    print("Conexión desde", addr)
    op = OperadorTramas(conn)
    try:
        while True:
            saludo = conn.recv(1)
            if not saludo:
                raise ConnectionError("El cliente ha cerrado")
            if saludo == ENQ:
                conn.sendall(ACK)
                break
            conn.sendall(NACK)

        man = ManejadorDatos()
        while True:
            while True:
                try:
                    datos = op.siguiente()
                except (ValueError):
                    conn.sendall(NACK)
                    continue
                conn.sendall(ACK)
                man.leerDatos(datos)
                break

            trama_respuesta = op.construir_trama(man.respuesta.encode())
            while True:
                conn.sendall(trama_respuesta)
                if conn.recv(1) == ACK:
                    break

            if man.comando == "BYE":
                break

        if conn.recv(1) == EOT:
            print ("Recibido EOT, se cierra la conexion con", addr)

    except (ConnectionError):
        print("Se ha cerrado la conexión con", addr)

    finally:
        conn.close()

def main():

    db = sqlite3.connect(DB_PATH)
    db.execute('''CREATE TABLE IF NOT EXISTS operators(id TEXT PRIMARY KEY, name NOT NULL, active INTEGER NOT NULL DEFAULT 1);''')

    # status puede valer disponible, regando, fuga, fuera_servicio, offline
    db.execute('''CREATE TABLE IF NOT EXISTS stations (id TEXT PRIMARY KEY, status TEXT NOT NULL,
    blocked INTEGER NOT NULL DEFAULT 0, ultimo_registro TEXT);''') # Ultimo registro en formato de fecha y hora

    db.execute('''CREATE TABLE IF NOT EXISTS logs_riego (id INTEGER PRIMARY KEY AUTOINCREMENT, ws_id TEXT NOT NULL REFERENCES stations(id),
    operator_id TEXT REFERENCES operators(id), started_at TEXT, ended_at TEXT, volumen_l REAL, salida TEXT);''')

    db.commit()
    db.close()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
        soc.bind((IP, PORT))
        soc.listen()
        print(f"Servidor escuchando en {IP}:{PORT}")
        while True:
            conn, addr = soc.accept()
            conn.settimeout(30) # Aseguramos que van a escribir en el socket
            # Creamos un hilo por cada socket
            threading.Thread(target=atender, args=(conn,addr), daemon=True).start()

if __name__ == "__main__":
    main()

