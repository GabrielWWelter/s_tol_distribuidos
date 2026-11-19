import socket
import struct

# etapa1
MSG_DISCOVERY = 1
MSG_DISCOVERY_RESP = 2
MSG_REQ = 3
MSG_ACK = 4

'''
Ordenação Big-Endian (!) para garantir que todos os computadores interpretem da mesma forma
'''

# 1 byte identificador de mensagem
def pack_discovery():
    return struct.pack("!B", MSG_DISCOVERY)

# 1 byte identificador de mensagem
def pack_discovery_resp():
    return struct.pack("!B", MSG_DISCOVERY_RESP)

# Adaptado da Etapa 1: acrescenta 8 bytes de client_id.
# 1 byte identificador de mensagem,
# 8 bytes client_id (unsigned int 64 bits),
# 8 bytes id_req (unsigned int 64 bits),
# 8 bytes valor (unsigned int 64 bits)
def pack_req(client_id, id_req, valor):
    return struct.pack("!BQQQ", MSG_REQ, client_id, id_req, valor)

def unpack_req(dados):
    _, client_id, id_req, valor = struct.unpack("!BQQQ", dados)
    return client_id, id_req, valor

# Adaptado da Etapa 1: acrescenta 8 bytes de client_id.
# 1 byte identificador de mensagem,
# 8 bytes client_id (unsigned int 64 bits),
# 8 bytes id_req (unsigned int 64 bits),
# 8 bytes num_reqs (unsigned int 64 bits),
# 8 bytes total_sum (unsigned int 64 bits)
def pack_ack(client_id, id_req, num_reqs, total_sum):
    return struct.pack("!BQQQQ", MSG_ACK, client_id, id_req, num_reqs, total_sum)

def unpack_ack(dados):
    _, client_id, id_req, num_reqs, total_sum = struct.unpack("!BQQQQ", dados)
    return client_id, id_req, num_reqs, total_sum


# --------- etapa2 -----------
MSG_ORDER_MULTICAST = 10
MSG_RM_ACK = 11
MSG_HEARTBEAT = 12
MSG_NACK = 13
MSG_DISCOVERY_FWD = 14
MSG_TOPOLOGY = 15
MSG_SERVER_DISCOVERY = 16
MSG_SERVER_DISCOVERY_RESP = 17
MSG_RM_JOIN = 18
MSG_STATE_SYNC = 19
MSG_ELECTION = 20
MSG_COORDINATOR = 21
MSG_ELECTION_ACK = 22

# logs
COLOR_RESET = "\033[0m"
COLOR_FE = "\033[96m"        # ciano (Front-End)
COLOR_RM = "\033[92m"        # verde (Replica Manager)
COLOR_ELECTION = "\033[95m"  # magenta (Eleicao)
COLOR_LEADER = "\033[93m"    # amarelo (Novo Lider)
COLOR_FAIL = "\033[91m"      # vermelho (Falhas/Timeouts)

def colorir(texto, cor):
    return f"{cor}{texto}{COLOR_RESET}"

# 1 byte tipo, 8 bytes seq_id, 8 bytes client_id, 8 bytes id_req, 8 bytes valor
def pack_order_multicast(seq_id, client_id, id_req, valor):
    return struct.pack("!BQQQQ", MSG_ORDER_MULTICAST, seq_id, client_id, id_req, valor)

def unpack_order_multicast(dados):
    _, seq_id, client_id, id_req, valor = struct.unpack("!BQQQQ", dados)
    return seq_id, client_id, id_req, valor

# 1 byte tipo, 8 bytes seq_id, 8 bytes client_id, 8 bytes id_req,
# 8 bytes num_reqs, 8 bytes total_sum, 4 bytes rm_id
def pack_rm_ack(seq_id, client_id, id_req, num_reqs, total_sum, rm_id):
    return struct.pack("!BQQQQQI", MSG_RM_ACK, seq_id, client_id, id_req, num_reqs, total_sum, rm_id)

def unpack_rm_ack(dados):
    _, seq_id, client_id, id_req, num_reqs, total_sum, rm_id = struct.unpack("!BQQQQQI", dados)
    return seq_id, client_id, id_req, num_reqs, total_sum, rm_id

# 1 byte tipo, 4 bytes leader_id, 8 bytes last_seq
def pack_heartbeat(leader_id, last_seq):
    return struct.pack("!BIQ", MSG_HEARTBEAT, leader_id, last_seq)

def unpack_heartbeat(dados):
    _, leader_id, last_seq = struct.unpack("!BIQ", dados)
    return leader_id, last_seq

# 1 byte tipo, 4 bytes sender_id, 4 bytes highest_id
def pack_election(sender_id, highest_id):
    return struct.pack("!BII", MSG_ELECTION, sender_id, highest_id)

def unpack_election(dados):
    _, sender_id, highest_id = struct.unpack("!BII", dados)
    return sender_id, highest_id

# 1 byte tipo, 4 bytes leader_id
def pack_coordinator(leader_id):
    return struct.pack("!BI", MSG_COORDINATOR, leader_id)

def unpack_coordinator(dados):
    _, leader_id = struct.unpack("!BI", dados)
    return leader_id

# Protocolos de NACK para evitar que um RM trave permanentemente em caso de perda de pacotes
def pack_nack(expected_seq, rm_id):
    return struct.pack("!BQI", MSG_NACK, expected_seq, rm_id)

def unpack_nack(dados):
    _, expected_seq, rm_id = struct.unpack("!BQI", dados)
    return expected_seq, rm_id

# Encaminhamento de descoberta broadcast recebida por um RM para o FE
# 1 byte tipo, 4 bytes client_ip (IPv4), 2 bytes client_port
def pack_discovery_fwd(client_ip, client_port):
    return struct.pack("!B4sH", MSG_DISCOVERY_FWD, socket.inet_aton(client_ip), client_port)

def unpack_discovery_fwd(dados):
    _, ip_bytes, client_port = struct.unpack("!B4sH", dados)
    return socket.inet_ntoa(ip_bytes), client_port
def pack_election_ack(sender_id):
    return struct.pack("!BI", MSG_ELECTION_ACK, sender_id)

def unpack_election_ack(dados):
    _, sender_id = struct.unpack("!BI", dados)
    return sender_id

# 1 byte tipo, 4 bytes sender_id
def pack_server_discovery(sender_id):
    return struct.pack("!BI", MSG_SERVER_DISCOVERY, sender_id)

def unpack_server_discovery(dados):
    _, sender_id = struct.unpack("!BI", dados)
    return sender_id

# 1 byte tipo, 4 bytes leader_id
def pack_server_discovery_resp(leader_id):
    return struct.pack("!BI", MSG_SERVER_DISCOVERY_RESP, leader_id)

def unpack_server_discovery_resp(dados):
    _, leader_id = struct.unpack("!BI", dados)
    return leader_id

# 1 byte tipo, 4 bytes rm_id, 2 bytes cluster_port
def pack_rm_join(rm_id, cluster_port):
    return struct.pack("!BIH", MSG_RM_JOIN, rm_id, cluster_port)

def unpack_rm_join(dados):
    _, rm_id, cluster_port = struct.unpack("!BIH", dados)
    return rm_id, cluster_port

# 1 byte tipo, 4 bytes leader_id, 2 bytes qtd_nos, lista de IPs (4 bytes cada)
def pack_topology(leader_id, ip_list):
    payload = struct.pack("!BIH", MSG_TOPOLOGY, leader_id, len(ip_list))
    for ip in ip_list:
        payload += socket.inet_aton(ip)
    return payload

def unpack_topology(dados):
    _, leader_id, qtd = struct.unpack("!BIH", dados[:7])
    ips = []
    offset = 7
    for _ in range(qtd):
        ip_str = socket.inet_ntoa(dados[offset : offset + 4])
        ips.append(ip_str)
        offset += 4
    return leader_id, ips

# 1 byte tipo, 8 bytes next_seq, 8 bytes num_reqs, 8 bytes total_sum
def pack_state_sync(next_seq, num_reqs, total_sum):
    return struct.pack("!BQQQ", MSG_STATE_SYNC, next_seq, num_reqs, total_sum)

def unpack_state_sync(dados):
    _, next_seq, num_reqs, total_sum = struct.unpack("!BQQQ", dados)
    return next_seq, num_reqs, total_sum
