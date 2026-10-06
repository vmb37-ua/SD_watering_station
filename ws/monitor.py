import socket,sys


STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

def construir_trama(data):
    lrc = 0
    for b in data:
        lrc ^= b
    return STX + data + ETX + bytes([lrc])

def enviar(sock, datos):
    # manda la trama y espera 1 byte, si es NACK la manda otra vez ja
    trama = construir_trama(datos.encode())
    print(trama)
    while True:
        sock.sendall(trama)
        resp = sock.recv(1)
        if resp == ACK:
            print("La central ha recibido el", datos)
            break
        elif not resp:
            print("La central se ha caido")
            sys.exit(1)
        else:
            print("NACK, reenvio la trama")

def recibir(sock):
    buffer = b''
    while True:
        # la trama esta entera cuando tengo el ETX y un byte mas (el LRC)
        fin = buffer.find(ETX)
        if fin != -1 and len(buffer) >= fin + 2:
            datos = buffer[1:fin]   # me salto el STX
            lrc = 0
            for b in datos:
                lrc ^= b
            if lrc == buffer[fin+1]:
                sock.sendall(ACK)
                return datos.decode()
            # si el LRC no cuadra pido que la mande otra vez
            print("LRC mal, mando NACK")
            sock.sendall(NACK)
            buffer = b''
        trozo = sock.recv(1024)
        if not trozo:
            print("La central se ha caido")
            sys.exit(1)
        buffer += trozo

def main():
    #moitor.py <puerto_engine> <ip_central:puerto> <id_ws> 
    if len(sys.argv) != 4:      
        print("Uso: python moitor.py <puerto_engine> <ip_central:puerto> <id_ws>") 
        sys.exit(1)
        
    puerto_engine = int(sys.argv[1])
    dir_central = (sys.argv[2]).split(":")
    ip_central = dir_central[0]
    puerto_central = int(dir_central[1])
    id_ws = sys.argv[3]
    

    print(f"Monitor {id_ws} --> central  {ip_central}:{puerto_central},engine en {puerto_engine}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sunflower:
        sunflower.connect((ip_central,puerto_central))
        sunflower.sendall(ENQ)
        respuesta = sunflower.recv(1)
        if respuesta == ACK:
            print("Todo bien")
        else:
            print("Algo anda mal")
            sys.exit(1)
        enviar(sunflower, "AUTH#" + id_ws)
        respuesta = recibir(sunflower)
        print("Respuesta de la central:", respuesta)

        # la central no cierra hasta que le llega el BYE
        enviar(sunflower, "BYE#" + id_ws)
        respuesta = recibir(sunflower)
        print("Respuesta de la central:", respuesta)

        sunflower.sendall(EOT)   # fin de la transmision

if __name__ == "__main__":
    main()
    
