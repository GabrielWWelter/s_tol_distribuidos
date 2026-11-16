import sys
import socket
import threading
import queue
import secrets
from datetime import datetime
import protocolo

# tempo de espera para o protocolo stop-and-wait
TIMEOUT = 0.010 # 10 milisegundos
TENTATIVAS_ANTES_REDESCOBERTA = 100 # valor arbitário, talvez mudar isso quando definir o heartbeat

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def interface_servidor_descoberto(server_ip):
    print(f"{get_timestamp()} server_addr {server_ip}", flush=True)

def interface_resposta_recebida(server_ip, id_req, valor, num_reqs, total_sum):
    ts = get_timestamp()
    print(f"{ts} server {server_ip} id_req {id_req} value {valor} num_reqs {num_reqs} total_sum {total_sum}", flush=True)

# Novo na Etapa 2: identidade estável durante toda a execução do cliente.
def gerar_client_id():
    client_id = 0
    while client_id == 0:
        client_id = secrets.randbits(64)
    return client_id

def subservico_interface_leitura(fila):
    try:
        for linha in sys.stdin:
            linha = linha.strip()
            if linha:
                try:
                    valor = int(linha)
                    if 0 < valor <= 0xFFFFFFFFFFFFFFFF: # valor positivo de 64 bits
                        fila.put(valor) # add na fila
                except ValueError:
                    pass
    except Exception:
        pass
    finally:
        fila.put(None)

def subservico_descoberta(sock, porta):
    msg = protocolo.pack_discovery()
    sock.settimeout(0.5) #500 ms
    destinos = [('<broadcast>', porta), ('127.0.0.1', porta)]

    while True:
        # testa rede local e localhost
        for dest in destinos:
            try:
                sock.sendto(msg, dest)
            except OSError:
                pass
        try:
            # payload, endereço (ip, porta)
            dados, addr = sock.recvfrom(1024)
            # se receber dados, e for do tipo descoberta, retorna o IP o servidor
            if dados and dados[0] == protocolo.MSG_DISCOVERY_RESP:
                return addr[0]
        except (socket.timeout, TimeoutError, OSError):
            continue


def subservico_processamento(sock, server_addr, client_id, id_req, valor, server_ip):
    pacote = protocolo.pack_req(client_id, id_req, valor)
    sock.settimeout(TIMEOUT)
    tentativas = 0

    while True:
        if tentativas == TENTATIVAS_ANTES_REDESCOBERTA:
            server_ip = subservico_descoberta(sock, server_addr[1])
            server_addr = (server_ip, server_addr[1])
            interface_servidor_descoberto(server_ip)
            sock.settimeout(TIMEOUT)
            tentativas = 0
        try:
            # envia o pacote para o servidor
            sock.sendto(pacote, server_addr)
            tentativas += 1
            while True:
                resp, addr = sock.recvfrom(1024)
                # se receber um pacote do tipo ACK, desempacota
                if resp and resp[0] == protocolo.MSG_ACK:
                    try:
                        ack_client_id, ack_id, num_reqs, total_sum = protocolo.unpack_ack(resp)
                    except Exception:
                        continue
                    # ignora um ACK pertencente a outra execução de cliente
                    if ack_client_id != client_id:
                        continue
                    # se o id_req do ack for o mesmo que o enviado, imprime a resposta
                    if ack_id == id_req:
                        interface_resposta_recebida(server_ip, id_req, valor, num_reqs, total_sum)
                        return server_addr
                    # se o id_req do ack for menor ao enviado, descarta e continua esperando por até 10 ms
                    elif ack_id < id_req:
                        continue
        except (socket.timeout, TimeoutError, OSError):
            # se o timeout de 10 ms expirar ou houver erro temporario de rede, reenvia o pacote
            continue

def main():
    if len(sys.argv) < 2:
        print(f"Uso: python3 {sys.argv[0]} <porta>")
        sys.exit(1)

    porta = int(sys.argv[1]) # via linha de comando
    client_id = gerar_client_id()

    # cria socket UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # permite broadcast
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    try:
        server_ip = subservico_descoberta(sock, porta)
        interface_servidor_descoberto(server_ip)

        # para comunicação entre thread principal e leitora, sem condições de corrida
        fila = queue.Queue()
        # thread leitora
        t_leitora = threading.Thread(target=subservico_interface_leitura, args=(fila,), daemon=True)
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

            # passa um número da fila para processamento
            server_addr = subservico_processamento(sock, server_addr, client_id, id_req, valor, server_addr[0])
            id_req += 1

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()

if __name__ == "__main__":
    main()
