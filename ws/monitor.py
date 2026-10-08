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
    # Mandamos la trama y esperamos 1 byte, si es NACK la mandamos otra vez
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
        # La trama esta entera cuando tenemos el ETX y un byte mas, que es el LRC
        fin = buffer.find(ETX)
        if fin != -1 and len(buffer) >= fin + 2:
            datos = buffer[1:fin] # Nos saltamos el STX
            lrc = 0
            for b in datos:
                lrc ^= b
            if lrc == buffer[fin+1]:
                sock.sendall(ACK)
                return datos.decode()
            # Si el LRC no cuadra pedimos que la manden otra vez
            print("LRC mal, mando NACK")
            sock.sendall(NACK)
            buffer = b''
        trozo = sock.recv(1024)
        if not trozo:
            raise ConnectionError("han cerrado la conexion")
        buffer += trozo

def conectar_central(ip, puerto, id_ws, ubicacion):
    # Una sola conexion que se queda abierta: saludo, REG y AUTH
    # La central solo deja mandar ALERT y CLEAR por la misma conexion por la que se hizo el AUTH
    try:
        sunflower = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sunflower.settimeout(5)
        sunflower.connect((ip,puerto))
        sunflower.sendall(ENQ)
        if sunflower.recv(1) != ACK:
            print("Algo anda mal")
            sunflower.close()
            return None
        # Nos registramos siempre, si la estacion ya existe la central solo le cambia la ubicacion
        enviar(sunflower, "REG#" + id_ws + "#" + ubicacion)
        print("Respuesta de sunflower:", recibir(sunflower))
        enviar(sunflower, "AUTH#" + id_ws)
        respuesta = recibir(sunflower)
        print("Respuesta de sunflower:", respuesta)
        if not respuesta.startswith("OK_AUTH"):
            sunflower.close()
            return None
        return sunflower
    except OSError:
        print("No puedo hablar con sunflower")
        return None

def hablar_central(sunflower, mensaje):
    # Se manda por la conexion que ya esta abierta
    if sunflower is None:
        return None
    try:
        enviar(sunflower, mensaje)
        return recibir(sunflower)
    except OSError:
        print("No puedo hablar con sunflower")
        sunflower.close()
        return None

def abrir_servidor(puerto):
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#crear socket
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)#liberar socket rapidamente , por si es necessario al cerrar el monitor
    servidor.bind(('0.0.0.0', puerto))#asociar socketal puerto, para que escuche por cualquier tarjeta de red, que escuche todo
    servidor.listen(1)#el cocket empieza escuchar , solo 1 puede eperar en la cola
    servidor.settimeout(1)#el accept solo espera 1 segundo, si no el monitor se queda parado y no manda los HEART a la central
    print("Esperando al engine en el puerto", puerto)
    return servidor

def main():
    # monitor.py <puerto_engine> <ip_central:puerto> <id_ws> <ubicacion>
    if len(sys.argv) != 4 and len(sys.argv) != 5:
        print("Uso: python monitor.py <puerto_engine> <ip_central:puerto> <id_ws> <ubicacion>")
        sys.exit(1)

    puerto_engine = int(sys.argv[1])
    dir_central = (sys.argv[2]).split(":")
    ip_central = dir_central[0]
    puerto_central = int(dir_central[1])
    id_ws = sys.argv[3]
    # La ubicacion la pide el REG de la central, si no la pasan ponemos una cualquiera
    ubicacion = "Sin ubicacion"
    if len(sys.argv) == 5:
        ubicacion = sys.argv[4]

    print(f"Monitor {id_ws} --> sunflower  {ip_central}:{puerto_central},engine en {puerto_engine}")

    central = conectar_central(ip_central, puerto_central, id_ws, ubicacion)
    if central is None:
        sys.exit(1)

    servidor = abrir_servidor(puerto_engine)
    donkey = None # El socket del engine, None si todavia no se ha conectado

    averia = False
    huevos = 0
    vueltas = 0
    while True:
        vueltas += 1

        # Si se ha caido la central intentamos conectar otra vez
        if central is None:
            central = conectar_central(ip_central, puerto_central, id_ws, ubicacion)
            if central is not None and averia:
                # La central se olvida de la averia al volver a autenticarnos, hay que repetirsela
                hablar_central(central, "ALERT#" + id_ws + "#LEAK")

        # Cada 5 vueltas le decimos a la central que seguimos vivos, si no corta a los 30 segundos
        if vueltas % 5 == 0 and central is not None:
            if hablar_central(central, "HEART#" + id_ws) is None:
                central = None

        if donkey is None:
            # Miramos si ha llegado el engine, el accept espera 1 segundo como mucho
            try:
                donkey, addr = servidor.accept()
                donkey.settimeout(3) # Si en 3 segundos no contesta lo damos por caido
                print("Engine conectado desde", addr)
                # Lo primero es decirle al engine que estacion es, el id solo lo sabe el monitor
                enviar(donkey, "ID#" + id_ws + "#" + ubicacion)
            except OSError:
                pass
            continue

        # Cada segundo le preguntamos al engine si esta bien
        huevos += 1
        try:
            # Le mandamos tambien el numero para que el engine lleve la misma cuenta
            enviar(donkey, "SALUD#" + str(huevos))
            respuesta = recibir(donkey)
        except OSError:
            # No contesta o ha cerrado la conexion
            respuesta = "ROTOS"
        print(huevos, "huevos", respuesta)

        if respuesta != "OK" and not averia:
            averia = True
            print("Averia, aviso a sunflower")
            respuesta_central = hablar_central(central, "ALERT#" + id_ws + "#LEAK")
            print("Respuesta de sunflower:", respuesta_central)
            if respuesta_central is None:
                central = None
        elif respuesta == "OK" and averia:
            averia = False
            print("Averia resuelta, aviso a sunflower")
            respuesta_central = hablar_central(central, "CLEAR#" + id_ws)
            print("Respuesta de sunflower:", respuesta_central)
            if respuesta_central is None:
                central = None

        if respuesta == "ROTOS":
            donkey.close()
            donkey = None
            print("Esperando al engine en el puerto", puerto_engine)
        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Monitor apagado")
