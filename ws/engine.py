import socket,sys

STX = b'\x02'
ETX = b'\x03'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'

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
        monitor.connect((ip_monitor,puerto_monitor))
        print("Conectado al monitor")

if __name__ == "__main__":
    main()
