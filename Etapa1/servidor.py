import sys
import socket
from datetime import datetime
import protocolo

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    if len(sys.argv) < 2:
        print(f"Uso: python3 {sys.argv[0]} <porta>")
        sys.exit(1)

    porta = int(sys.argv[1])

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', porta))

    # Tabela de clientes (Tabela 1) e acumuladores globais (Tabela 2)
    clientes = {}
    num_reqs = 0
    total_sum = 0

    print(f"{get_timestamp()} num_reqs 0 total_sum 0", flush=True)

    try:
        while True:
            dados, addr = sock.recvfrom(1024)
            if not dados:
                continue

            tipo = dados[0]
            client_ip = addr[0]

            # Subservico de Descoberta
            if tipo == protocolo.MSG_DISCOVERY:
                if client_ip not in clientes:
                    clientes[client_ip] = {
                        'last_req': 0,
                        'last_num_reqs': 0,
                        'last_total_sum': 0
                    }
                sock.sendto(protocolo.pack_discovery_resp(), addr)

            # Subservico de Processamento
            elif tipo == protocolo.MSG_REQ:
                try:
                    id_req, valor = protocolo.unpack_req(dados)
                except Exception:
                    continue

                if client_ip not in clientes:
                    clientes[client_ip] = {
                        'last_req': 0,
                        'last_num_reqs': 0,
                        'last_total_sum': 0
                    }

                cliente = clientes[client_ip]

                # Nova requisicao em ordem
                if id_req == cliente['last_req'] + 1:
                    num_reqs = (num_reqs + 1) & 0xFFFFFFFFFFFFFFFF
                    total_sum = (total_sum + valor) & 0xFFFFFFFFFFFFFFFF

                    cliente['last_req'] = id_req
                    cliente['last_num_reqs'] = num_reqs
                    cliente['last_total_sum'] = total_sum

                    ts = get_timestamp()
                    print(f"{ts} client {client_ip} id_req {id_req} value {valor} num_reqs {num_reqs} total_sum {total_sum}", flush=True)

                    ack = protocolo.pack_ack(id_req, num_reqs, total_sum)
                    sock.sendto(ack, addr)

                # Requisicao duplicada
                elif id_req <= cliente['last_req']:
                    ts = get_timestamp()
                    print(f"{ts} client {client_ip} DUP!! id_req {id_req} value {valor} num_reqs {cliente['last_num_reqs']} total_sum {cliente['last_total_sum']}", flush=True)

                    ack = protocolo.pack_ack(cliente['last_req'], cliente['last_num_reqs'], cliente['last_total_sum'])
                    sock.sendto(ack, addr)

                # id_req > last_req + 1
                else:
                    ack = protocolo.pack_ack(cliente['last_req'], cliente['last_num_reqs'], cliente['last_total_sum'])
                    sock.sendto(ack, addr)

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()

if __name__ == "__main__":
    main()
