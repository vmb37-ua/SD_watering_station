import socket,sys


STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

def main():
    #moitor.py <puerto_engine> <ip_central:puerto> <id_ws> <Lebron_James>
    if len(sys.argv) != 5:      #----> 4 + (Lebron_James)
        print("Uso: python moitor.py <puerto_engine> <ip_central:puerto> <id_ws> <Lebron_James>") 
        sys.exit(1)
        
    puerto_engine = int(sys.argv[1])
    dir_central = (sys.argv[2]).split(":")
    ip_central = dir_central[0]
    puerto_central = int(dir_central[1])
    id_ws = sys.argv[3]
    lb = (sys.argv[4])

    print(f"Monitor {id_ws} --> central  {ip_central}:{puerto_central},engine en {puerto_engine},goat --> {lb}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sunflower:
        sunflower.connect((ip_central,puerto_central))
        sunflower.sendall(ENQ)
        respuesta = sunflower.recv(1)
        if respuesta == ACK:
            print("Todo joya :D")
        else:
            print("Algo anda mal")
            sys.exit(1)



if __name__ == "__main__":
    main()
    
