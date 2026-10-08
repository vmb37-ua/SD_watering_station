import socket, sys, threading, time, os

STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

ko = False # Si esta a True hay fuga y al monitor se le contesta KO
id_ws = None # El id y la ubicacion de la estacion los manda el monitor al conectarnos
ubicacion = None

def construir_trama(datos):
    lrc = 0
    for b in datos:
        lrc ^= b
    return STX + datos + ETX + bytes([lrc])

def enviar(sock, datos):
    # Igual que en el monitor, se manda la trama y se espera el ACK
    trama = construir_trama(datos.encode())
    while True:
        sock.sendall(trama)
        resp = sock.recv(1)
        if resp == ACK:
            break
        elif not resp:
            raise ConnectionError("el monitor ha cerrado")
        else:
            print("NACK, reenvio la trama")

def recibir(sock):
    buffer = b''
    while True:
        fin = buffer.find(ETX)
        if fin != -1 and len(buffer) >= fin + 2:
            datos = buffer[1:fin] # Nos saltamos el STX
            lrc = 0
            for b in datos:
                lrc ^= b
            if lrc == buffer[fin+1]:
                sock.sendall(ACK)
                return datos.decode()
            print("LRC mal, mando NACK")
            sock.sendall(NACK)
            buffer = b''
        trozo = sock.recv(1024)
        if not trozo:
            raise ConnectionError("el monitor ha cerrado")
        buffer += trozo

def leer_teclado():
    # Va en un hilo aparte porque el input se queda parado esperando y no dejaria contestar al monitor
    global ko
    while True:
        tecla = input()
        if tecla == "k":
            ko = not ko
            if ko:
                print("FUGA simulada, ahora contesto KO al monitor")
            else:
                print("Fuga arreglada, vuelvo a contestar OK")

def main():
    global id_ws, ubicacion
    # engine.py <ip_kafka:puerto> <ip_monitor:puerto>
    if len(sys.argv) != 3:
        print("Uso: python engine.py <ip_kafka:puerto> <ip_monitor:puerto>")
        sys.exit(1)

    dir_kafka = sys.argv[1]
    dir_monitor = (sys.argv[2]).split(":")
    ip_monitor = dir_monitor[0]
    puerto_monitor = int(dir_monitor[1])

    print(f"Engine --> monitor {ip_monitor}:{puerto_monitor}, kafka en {dir_kafka}")

    threading.Thread(target=leer_teclado, daemon=True).start()
    print("Escribe k y enter para simular una fuga (y otra vez para arreglarla)")

    while True:
        # Si el monitor no esta arrancado o se cae, lo volvemos a intentar cada 2 segundos
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as monitor:
                monitor.connect((ip_monitor,puerto_monitor))
                print("Conectado al monitor")

                # El monitor pregunta cada segundo y le contestamos como estamos
                while True:
                    mensaje = recibir(monitor)
                    if mensaje.startswith("SALUD"):
                        # El numero de huevos lo manda el monitor, SALUD#<numero>
                        huevos = mensaje.split("#")[1]
                        if ko:
                            enviar(monitor, "KNOCKOUT")
                            print(huevos, "huevos KO")
                        else:
                            enviar(monitor, "OK")
                            print(huevos, "huevos")
                    elif mensaje.startswith("ID"):
                        # ID#<id>#<ubicacion>
                        partes = mensaje.split("#")
                        id_ws = partes[1]
                        ubicacion = partes[2]
                        print(f"Soy la estacion {id_ws} ({ubicacion})")
        except OSError:
            print("No hay monitor,GG, lo intento otra vez")
            time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Engine apagado")
        # Se cierra con os._exit en vez de un exit normal porque con el normal se quedaba colgado,
        # el hilo del teclado sigue esperando una tecla
        os._exit(0)
