import sys
import socket
from datetime import datetime
import protocolo

class EstadoServidor:
    def __init__(self):
        self.clientes = {}
        self.num_reqs = 0
        self.total_sum = 0

estado = EstadoServidor()

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def interface_inicializacao():
    print(f"{get_timestamp()} num_reqs 0 total_sum 0", flush=True)

def interface_nova_requisicao(client_ip, id_req, valor, num_reqs, total_sum):
    ts = get_timestamp()
    print(f"{ts} client {client_ip} id_req {id_req} value {valor} num_reqs {num_reqs} total_sum {total_sum}", flush=True)

def interface_requisicao_duplicada(client_ip, id_req, valor, num_reqs, total_sum):
    ts = get_timestamp()
    print(f"{ts} client {client_ip} DUP!! id_req {id_req} value {valor} num_reqs {num_reqs} total_sum {total_sum}", flush=True)

def subservico_descoberta(sock, addr, client_ip):
    if client_ip not in estado.clientes:
        estado.clientes[client_ip] = {
            'last_req': 0,
            'last_num_reqs': 0,
            'last_total_sum': 0
        }
    sock.sendto(protocolo.pack_discovery_resp(), addr)

def subservico_processamento(sock, dados, addr, client_ip):
    try:
        id_req, valor = protocolo.unpack_req(dados)
    except Exception:
        return

    if client_ip not in estado.clientes:
        estado.clientes[client_ip] = {
            'last_req': 0,
            'last_num_reqs': 0,
            'last_total_sum': 0
        }

    cliente = estado.clientes[client_ip]

    if id_req == cliente['last_req'] + 1:
        estado.num_reqs = (estado.num_reqs + 1) & 0xFFFFFFFFFFFFFFFF
        estado.total_sum = (estado.total_sum + valor) & 0xFFFFFFFFFFFFFFFF

        cliente['last_req'] = id_req
        cliente['last_num_reqs'] = estado.num_reqs
        cliente['last_total_sum'] = estado.total_sum

        interface_nova_requisicao(client_ip, id_req, valor, estado.num_reqs, estado.total_sum)

        ack = protocolo.pack_ack(id_req, estado.num_reqs, estado.total_sum)
        sock.sendto(ack, addr)

    elif id_req <= cliente['last_req']:
        interface_requisicao_duplicada(client_ip, id_req, valor, cliente['last_num_reqs'], cliente['last_total_sum'])

        ack = protocolo.pack_ack(cliente['last_req'], cliente['last_num_reqs'], cliente['last_total_sum'])
        sock.sendto(ack, addr)

    else:
        ack = protocolo.pack_ack(cliente['last_req'], cliente['last_num_reqs'], cliente['last_total_sum'])
        sock.sendto(ack, addr)

def main():
    if len(sys.argv) < 2:
        print(f"Uso: python3 {sys.argv[0]} <porta>")
        sys.exit(1)

    porta = int(sys.argv[1])

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', porta))

    interface_inicializacao()

    try:
        while True:
            dados, addr = sock.recvfrom(1024)
            if not dados:
                continue

            tipo = dados[0]
            client_ip = addr[0]

            if tipo == protocolo.MSG_DISCOVERY:
                subservico_descoberta(sock, addr, client_ip)
            elif tipo == protocolo.MSG_REQ:
                subservico_processamento(sock, dados, addr, client_ip)

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()

if __name__ == "__main__":
    main()
