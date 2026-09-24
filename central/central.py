import socket, sqlite3, threading, os

IP = '0.0.0.0' # Es necesario escuchar en todas las interfaces de red porque no sabemos cual asigna docker al exterior
PORT = os.getenv('PORT')

STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

class ComunicationException(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class ManejadorDatos():
    def leerDatos(self, datos):
        ... # TODO Lector de datos, que los organiza junto a sus argumentos, despues otra funcion se encarga de contestar

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
                    return datos
                else:
                    raise ValueError()
            trozo = self.conn.recv(4096)
            if not trozo:
                raise ConnectionError("El cliente ha cerrado")
            self.buffer += trozo

def atender(conn, addr):
    print("Conexión desde", addr)
    op = OperadorTramas(conn)
    try:
        while True:
            saludo = conn.recv(1)
            if saludo == ENQ:
                conn.sendall(ACK)
                break
            conn.sendall(NACK)

        man = ManejadorDatos()
        while True:
            try:
                datos = op.siguiente()
                conn.sendall(ACK)
                man.leerDatos(datos)
                break
            except (ValueError):
                conn.sendall(NACK)

        # TODO man.generarRespuesta() y acabar flujo de mensajes

    except (ConnectionError):
        print("Se ha cerrado la conexión con", addr)

    finally:
        conn.close()

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
        soc.bind((IP, PORT))
        soc.listen()
        print(f"Servidor escuchando en {IP}:{HOST}")
        while True:
            conn, addr = soc.accept()
            conn.settimeout(30) # Aseguramos que van a escribir en el socket
            # Creamos un hilo por cada socket
            threading.Thread(target=atender, args=(conn,addr), daemon=True).start()
            
