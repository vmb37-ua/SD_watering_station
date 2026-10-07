import socket,sys

STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

def construir_trama(datos):
    lrc = 0
    for b in datos:
        lrc ^= b
    return STX + datos + ETX + bytes([lrc])

def enviar(sock, datos):
    #igual que en el monitor , mando la trama y espero el ACK
    trama = construir_trama(datos.encode())
    while True:
        sock.sendall(trama)
        resp = sock.recv(1)
        if resp == ACK:
            break
        elif not resp:
            print("El monitor se ha caido")
            sys.exit(1)
        else:
            print("NACK, reenvio la trama")

def recibir(sock):
    buffer = b''
    while True:
        fin = buffer.find(ETX)
        if fin != -1 and len(buffer) >= fin + 2:
            datos = buffer[1:fin]#me salto el STX
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
            print("El monitor se ha caido")
            sys.exit(1)
        buffer += trozo

def main():
    #engine.py <ip_kafka:puerto> <ip_monitor:puerto>
    if len(sys.argv) != 3:
        print("Uso: python engine.py <ip_kafka:puerto> <ip_monitor:puerto>")
        sys.exit(1)

    dir_kafka = sys.argv[1]
    dir_monitor = (sys.argv[2]).split(":")
    ip_monitor = dir_monitor[0]
    puerto_monitor = int(dir_monitor[1])

    print(f"Engine --> monitor {ip_monitor}:{puerto_monitor}, kafka en {dir_kafka}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as monitor:
        try:
            monitor.connect((ip_monitor,puerto_monitor))
        except ConnectionRefusedError:
            print("El monitor no esta arrancado")
            sys.exit(1)
        print("Conectado al monitor")

        #el monitor me pregunta cada segundo y le contesto que estoy bien
        huevos = 0
        while True:
            mensaje = recibir(monitor)
            if mensaje == "SALUD":
                enviar(monitor, "OK")
                huevos += 1
                print(huevos, "huevos")

if __name__ == "__main__":
    main()
