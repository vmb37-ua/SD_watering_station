import socket, sqlite3, threading, os, argparse, itertools

from estado import Estado
from coordinador import Coordinador
from panel import arrancar_panel

IP = '0.0.0.0' # Es necesario escuchar en todas las interfaces de red porque no sabemos cual asigna docker al exterior
PORT = int(os.getenv('PORT', 5000))
DB_PATH = os.getenv('DB_PATH', "./database.db")

STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

coord = None # El cordinador debe instanciarse en main
cont_conexiones = itertools.count(1) # Usamos esta libreria para crear un iterador a 1

class ManejadorDatos():
    def __init__(self, conn_id, comando="", args=[], respuesta = ""):
        self.comando = comando
        self.args = args
        self.respuesta = respuesta
        self.conn_id = conn_id
        # Claramente no lo podemos poner a 0 o se confundiria con la estacion 0
        # Esto nos permite saber que estacion este autenticada
        self.ws_id = None 
    
    def leerDatos(self, datos:str):

        # Naturalmente el caracter # no se puede utilizar como un dato más, es el separador
        lista = datos.split("#")
        self.comando = lista[0]
        self.args = lista[1:]

        if self.comando != "HEART":
            print(f"[LOG] Central recibido --> {self.comando} - {self.args}")

        # Como es logico, una estacion solo puede hablar por su nombre, para ello se autentica y lo verificamos en estas condiciones
        # Antes de autenticarse solo se permite registrarse, autenticarse o despedirse
        if self.comando not in ("REG", "AUTH", "BYE") and self.ws_id is None:
            self.respuesta = f"ERR#{self.comando}#Estacion no autenticada"
            return
        # Una conexión solo puede hablar en nombre de la estación con la que se autenticó
        if self.comando not in ("REG", "AUTH") and self.ws_id is not None and self.args[:1] != [self.ws_id]:
            self.respuesta = f"ERR#{self.comando}#Id de estacion incorrecto"
            return

        # Aqui vamos a separar los tipos de mensaje y vamos a llamar a su manejador
        # (Echo de menos aqui el switch de C)
        if self.comando == "AUTH":
            self.respuesta = self.manejar_AUTH()
        elif self.comando == "REG":
            self.respuesta = self.manejar_REG()
        elif self.comando == "HEART":
            self.respuesta = self.manejar_HEART()
        elif self.comando == "ALERT":
            self.respuesta = self.manejar_ALERT()
        elif self.comando == "CLEAR":
            self.respuesta = self.manejar_CLEAR()
        elif self.comando == "BYE":
            self.respuesta = self.manejar_BYE()
        else:
            print("Comando no reconocido:", self.comando)
            self.respuesta = f"ERR#{self.comando}#Comando no reconocido"

    def manejar_AUTH(self):
        # AUTH#<estacion>
        # Solo podran autenticarse las estaciones que esten guardadas en BD
        estacion_num = self.args[0]
        estacion = coord.estado.estaciones.get(estacion_num)
        if estacion is None:
            return f"KO_AUTH#{estacion_num}#Estacion no registrada"
        self.ws_id = estacion_num
        # Ponemos en marcha la estacion
        coord.conectar(estacion_num, self.conn_id)
        return f"OK_AUTH#{estacion_num}#{estacion.ubicacion}"
        
    def manejar_REG(self):
        # REG#<estacion>#<ubicacion>
        try:
            coord.estado.registrar(self.args[0], self.args[1])
        except Exception as e:
            return f"ERR_REG#Error de Sqlite -> {e}"
        return f"OK_REG#{self.args[0]}"
    
    def manejar_ALERT(self):
        # ALERT#<estacion>#<motivo>
        coord.alerta(self.args[0], self.args[1])
        return f"OK_ALERT#{self.args[0]}"

    def manejar_CLEAR(self):
        # CLEAR#<estacion>
        coord.limpiar(self.args[0])
        return f"OK_CLEAR#{self.args[0]}"

    def manejar_BYE(self):
        # BYE#<estacion>
        return f"OK_BYE#{self.args[0]}"

    def manejar_HEART(self):
        # HEART#<estacion>
        return f"HEART_OK#{self.args[0]}"

class OperadorTramas:
    '''Quizá es complicarlo demasiado, pero hacer esto y no un .find para separar las tramas
    es mucho más robusto ya que evitamos la agrupación y la fragmentación de trams al no conocer exactamente su tamaño'''
    def __init__(self, conn):
        self.conn = conn
        self.buffer = b''

    def leer_control(self):
        while not self.buffer:
            trozo = self.conn.recv(4096)
            if not trozo:
                raise ConnectionError("El cliente ha cerrado")
            self.buffer += trozo
        b = self.buffer[:1]
        self.buffer = self.buffer[1:]
        return b

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
    # Vamos a utilizar el contador de conexiones para situar cada conexion con su estacion. Si por algun motivo la estacion duplicase conexiones se podrían evitar fallos
    man = ManejadorDatos(next(cont_conexiones))
    try:
        while True:
            saludo = op.leer_control()
            if saludo == ENQ:
                conn.sendall(ACK)
                break
            conn.sendall(NACK)

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
            for i in range(3):
                conn.sendall(trama_respuesta)
                if op.leer_control() == ACK:
                    break

            if man.comando == "BYE":
                break

        if op.leer_control() == EOT:
            print ("Recibido EOT, se cierra la conexion con", addr)

    # OSError para el timeout
    except (ConnectionError, OSError):
        print("Se ha cerrado la conexión con", addr)

    finally:
        conn.close()
        if man.ws_id is not None:
            coord.desconectar(man.ws_id, man.conn_id)

def main():

    global coord

    parser = argparse.ArgumentParser(prog="central")
    parser.add_argument("puerto", type=int, nargs="?", default=PORT, help="Puerto del servidor de sockets para los WM_WS_M")
    parser.add_argument("broker", nargs="?", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"), help="IP:puerto del bootstrap-server de Kafka")
    parser.add_argument("--http-port", type=int, default=int(os.getenv("HTTP_PORT", 8080)), help="Puerto del panel web")
    parser.add_argument("--db", default=DB_PATH, help="Ruta de la base de datos SQLite")
    argumentos = parser.parse_args()

    estado = Estado(argumentos.db)
    coord = Coordinador(estado, argumentos.broker)
    coord.arrancar()
    arrancar_panel(coord, argumentos.http_port)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
        # Cambiamos las opciones de la conexion para que no haya que esperar a que linux vuelva a liberar la direccion tras cerrar
        soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        soc.bind((IP, argumentos.puerto))
        soc.listen()
        print(f"Servidor escuchando en {IP}:{argumentos.puerto}")
        while True:
            conn, addr = soc.accept()
            conn.settimeout(30) # Aseguramos que van a escribir en el socket
            # Creamos un hilo por cada socket
            threading.Thread(target=atender, args=(conn,addr), daemon=True).start()

if __name__ == "__main__":
    main()

