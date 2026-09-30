import sys
import socket
import threading
import queue
from datetime import datetime
import protocolo

TIMEOUT = 0.010

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def descobrir_servidor(sock, porta):
    msg = protocolo.pack_discovery()
    sock.settimeout(0.5)
    destinos = [('<broadcast>', porta), ('127.0.0.1', porta)]

    while True:
        for dest in destinos:
            try:
                sock.sendto(msg, dest)
            except OSError:
                pass
        try:
            dados, addr = sock.recvfrom(1024)
            if dados and dados[0] == protocolo.MSG_DISCOVERY_RESP:
                return addr[0]
        except socket.timeout:
            continue

def ler_teclado(fila):
    try:
        for linha in sys.stdin:
            linha = linha.strip()
            if linha:
                try:
                    valor = int(linha)
                    if valor > 0:
                        fila.put(valor)
                except ValueError:
                    pass
    except Exception:
        pass
    finally:
        fila.put(None)

def main():
    if len(sys.argv) < 2:
        print(f"Uso: python3 {sys.argv[0]} <porta>")
        sys.exit(1)

    porta = int(sys.argv[1])

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    try:
        server_ip = descobrir_servidor(sock, porta)
        print(f"{get_timestamp()} server_addr {server_ip}", flush=True)

        fila = queue.Queue()
        t_leitora = threading.Thread(target=ler_teclado, args=(fila,), daemon=True)
        t_leitora.start()

        server_addr = (server_ip, porta)
        id_req = 1

        while True:
            try:
                valor = fila.get(timeout=0.1)
            except queue.Empty:
                continue

            if valor is None:
                break

            pacote = protocolo.pack_req(id_req, valor)
            sock.settimeout(TIMEOUT)

            ack_ok = False
            while not ack_ok:
                sock.sendto(pacote, server_addr)
                try:
                    while True:
                        resp, addr = sock.recvfrom(1024)
                        if resp and resp[0] == protocolo.MSG_ACK:
                            ack_id, num_reqs, total_sum = protocolo.unpack_ack(resp)
                            if ack_id == id_req:
                                ts = get_timestamp()
                                print(f"{ts} server {server_ip} id_req {id_req} value {valor} num_reqs {num_reqs} total_sum {total_sum}", flush=True)
                                id_req += 1
                                ack_ok = True
                                break
                            elif ack_id < id_req:
                                continue
                except socket.timeout:
                    continue

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()

if __name__ == "__main__":
    main()
