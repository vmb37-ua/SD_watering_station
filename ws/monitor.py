import socket,sys,time


STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

def construir_trama(david):
    lrc = 0
    for b in david:
        lrc ^= b
    return STX + david + ETX + bytes([lrc])

def enviar(sock, datos):
    # mandamos la trama y esperamos 1 byte, si es NACK la mandamos otra vez ja
    trama = construir_trama(datos.encode())
    while True:
        sock.sendall(trama)
        resp = sock.recv(1)
        if resp == ACK:
            break
        elif not resp:
            raise ConnectionError("han cerrado la conexion")
        else:
            print("NACK, reenvio la trama")

def recibir(sock):
    buffer = b''
    while True:
        #la trama esta entera cuando tengo el ETX y un byte mas que es el LRC
        fin = buffer.find(ETX)
        if fin != -1 and len(buffer) >= fin + 2:
            datos = buffer[1:fin]#me salto el STX
            lrc = 0
            for b in datos:
                lrc ^= b
            if lrc == buffer[fin+1]:
                sock.sendall(ACK)
                return datos.decode()
            #si el LRC no cuadra le pido que la mande otraves
            print("LRC mal, mando NACK")
            sock.sendall(NACK)
            buffer = b''
        trozo = sock.recv(1024)
        if not trozo:
            raise ConnectionError("han cerrado la conexion")
        buffer += trozo

def hablar_central(ip, puerto, id_ws, mensaje):
    #abro una conexion nueva para cada mensaje , saludo, mensaje, BYE y EOT
    #porque la central cierra la conexion si esta 30 segundos sin recivir nada
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sunflower:
            sunflower.connect((ip,puerto))
            sunflower.sendall(ENQ)
            if sunflower.recv(1) != ACK:
                print("Algo anda mal")
                return None
            enviar(sunflower, mensaje)
            respuesta = recibir(sunflower)
            #la central no cierra hasta que le llega el BYE
            enviar(sunflower, "BYE#" + id_ws)
            recibir(sunflower)
            sunflower.sendall(EOT)#fin de la transmision
            return respuesta
    except OSError:
        print("No puedo hablar con la central")
        return None

def esperar_engine(puerto):
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#crear socket
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)#liberar socket rapidamente , por si es necessario al cerrar el monitor
    servidor.bind(('0.0.0.0', puerto))#asociar socketal puerto, para que escuche por cualquier tarjeta de red, que escuche todo
    servidor.listen(1)#el cocket empieza escuchar , solo 1 puede eperar en la cola
    print("Esperando al engine en el puerto", puerto)
    conn, addr = servidor.accept()#se queda parado hasta que se conecta el engine, conn es el socket para hablar con el , ja
    print("Engine conectado desde", addr)
    servidor.close()
    conn.settimeout(3)#si en 3 segundos no contesta lo doy por caido
    return conn

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

    respuesta = hablar_central(ip_central, puerto_central, id_ws, "AUTH#" + id_ws)
    if respuesta is None:
        sys.exit(1)
    print("Respuesta de la central:", respuesta)

    donkey = esperar_engine(puerto_engine)

    #cada segundo le pregunto al engine si esta bien
    averia = False
    huevos = 0
    while True:
        try:
            enviar(donkey, "SALUD")
            respuesta = recibir(donkey)
        except OSError:
            #no contesta o a cerrado la conexion
            respuesta = "CAIDO"
        huevos += 1
        print(huevos, "huevos", respuesta)

        if respuesta != "OK" and not averia:
            averia = True
            print("Averia, aviso a la central")
            print("Respuesta de la central:", hablar_central(ip_central, puerto_central, id_ws, "ALERT#" + id_ws + "#LEAK"))
        elif respuesta == "OK" and averia:
            averia = False
            print("Averia resuelta, aviso a la central")
            print("Respuesta de la central:", hablar_central(ip_central, puerto_central, id_ws, "CLEAR#" + id_ws))

        if respuesta == "CAIDO":
            donkey.close()
            donkey = esperar_engine(puerto_engine)
        time.sleep(1)

if __name__ == "__main__":
    main()
    
