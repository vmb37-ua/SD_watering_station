import socket, sys, threading, time, os, json 
from confluent_kafka import Producer, Consumer

STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

T_COMMANDS = "watering.commands" # ordenes de central a estacion
T_EVENTS = "watering.events" # respuesta de estacion a central

ko = False # Si esta a True hay fuga y al monitor se le contesta KO
id_ws = None # El id y la ubicacion de la estacion los manda el monitor al conectarnos
ubicacion = None
hay_monitor = False # Para saber en el menu si estamos conectados al monitor
dir_kafka = None
productor = None # El productor de kafka, se crea cuando ya sabemos que estacion somos

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

def enviar_kafka(topic, mensaje):
    # A todos los mensajes les ponemos el id de la estacion y van en json, como los de la central
    mensaje["ws_id"] = id_ws
    productor.produce(topic, json.dumps(mensaje).encode())
    productor.poll(0)

def al_asignar(consumer, particiones):
    # Igual que en la central, nos colocamos al final del topic para no leer ordenes viejas
    for p in particiones:
        p.offset = consumer.get_watermark_offsets(p, timeout=10)[1]
    consumer.assign(particiones)
    # Ahora que ya estamos escuchando avisamos a la central de que la estacion esta conectada
    enviar_kafka(T_EVENTS, {"type": "ONLINE"})
    print("Conectado a kafka, ONLINE mandado a la central")

def escuchar_central():
    # Va en otro hilo, se queda leyendo las ordenes que manda la central por kafka
    global productor
    productor = Producer({"bootstrap.servers": dir_kafka})
    # Cada engine tiene su grupo, si estuvieran en el mismo las ordenes solo le llegarian a uno
    grupo = f"engine-{id_ws}-{int(time.time())}"
    consumer = Consumer({"bootstrap.servers": dir_kafka, "group.id": grupo, "auto.offset.reset": "latest"})
    consumer.subscribe([T_COMMANDS], on_assign=al_asignar)
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error de kafka:", msg.error())
            continue
        orden = json.loads(msg.value())
        # Las ordenes de todas las estaciones van por el mismo topic, solo nos interesan las nuestras
        if orden.get("ws_id") != id_ws:
            continue
        print("Orden de la central:", orden["type"])

def estado():
    # Devuelve en que estado esta la estacion para sacarlo en el menu
    if not hay_monitor:
        return "SIN MONITOR"
    if ko:
        return "FUGA"
    return "DISPONIBLE"

def pintar_menu():
    print(f"--- {id_ws} ({ubicacion}) estado: {estado()} ---")
    print("1. Pedir riego")
    print("2. Parar riego")
    if ko:
        print("k. Arreglar fuga")
    else:
        print("k. Simular fuga")
    print("3. Ver estado")
    print("0. Salir")

def menu():
    # Va en un hilo aparte porque el input se queda parado esperando y no dejaria contestar al monitor
    global ko
    # Hasta que el monitor no nos dice que estacion somos no sacamos el menu
    while id_ws is None:
        time.sleep(1)
    while True:
        pintar_menu()
        opcion = input()
        if opcion == "k":
            ko = not ko
            if ko:
                print("FUGA simulada, ahora contesto KO al monitor")
            else:
                print("Fuga arreglada, vuelvo a contestar OK")
        elif opcion == "1" or opcion == "2":
            print("Todavia no esta hecho")
        elif opcion == "3":
            print("Estado:", estado())
        elif opcion == "0":
            print("Engine apagado")
            os._exit(0)
        else:
            print("Opcion no valida")

def main():
    global id_ws, ubicacion, hay_monitor, dir_kafka
    # engine.py <ip_kafka:puerto> <ip_monitor:puerto>
    if len(sys.argv) != 3:
        print("Uso: python engine.py <ip_kafka:puerto> <ip_monitor:puerto>")
        sys.exit(1)

    dir_kafka = sys.argv[1]
    dir_monitor = (sys.argv[2]).split(":")
    ip_monitor = dir_monitor[0]
    puerto_monitor = int(dir_monitor[1])

    print(f"Engine --> monitor {ip_monitor}:{puerto_monitor}, kafka en {dir_kafka}")

    threading.Thread(target=menu, daemon=True).start()

    while True:
        # Si el monitor no esta arrancado o se cae, lo volvemos a intentar cada 2 segundos
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as monitor:
                monitor.connect((ip_monitor,puerto_monitor))
                print("Conectado al monitor")
                hay_monitor = True

                # El monitor pregunta cada segundo y le contestamos como estamos
                while True:
                    mensaje = recibir(monitor)
                    if mensaje.startswith("SALUD"):
                        # El numero de huevos lo manda el monitor, SALUD#<numero>
                        # Los prints estan comentados para que no tapen el menu, los huevos ya salen en el monitor
                        #huevos = mensaje.split("#")[1]
                        if ko:
                            enviar(monitor, "KNOCKOUT")
                            #print(huevos, "huevos KO")
                        else:
                            enviar(monitor, "OK")
                            #print(huevos, "huevos")
                    elif mensaje.startswith("ID"):
                        # ID#<id>#<ubicacion>
                        partes = mensaje.split("#")
                        primera_vez = id_ws is None
                        id_ws = partes[1]
                        ubicacion = partes[2]
                        print(f"Soy la estacion {id_ws} ({ubicacion})")
                        # Kafka se arranca solo la primera vez, si el monitor se cae y vuelve ya esta arrancado
                        if primera_vez:
                            threading.Thread(target=escuchar_central, daemon=True).start()
        except OSError:
            hay_monitor = False
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
