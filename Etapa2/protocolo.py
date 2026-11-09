import struct

# etapa1
MSG_DISCOVERY = 1
MSG_DISCOVERY_RESP = 2
MSG_REQ = 3
MSG_ACK = 4

# 1 byte identificador de mensagem
def pack_discovery():
    return struct.pack("!B", MSG_DISCOVERY)

# 1 byte identificador de mensagem
def pack_discovery_resp():
    return struct.pack("!B", MSG_DISCOVERY_RESP)

# 1 byte identificador de mensagem, 
# 8 bytes id_req (unsigned int 64 bits), 
# 8 bytes valor (unsigned int 64 bits)
def pack_req(id_req, valor):
    return struct.pack("!BQQ", MSG_REQ, id_req, valor)

def unpack_req(dados):
    _, id_req, valor = struct.unpack("!BQQ", dados)
    return id_req, valor

# 1 byte identificador de mensagem, 
# 8 bytes id_req (unsigned int 64 bits), 
# 8 bytes num_reqs (unsigned int 64 bits), 
# 8 bytes total_sum (unsigned int 64 bits)
def pack_ack(id_req, num_reqs, total_sum):
    return struct.pack("!BQQQ", MSG_ACK, id_req, num_reqs, total_sum)

def unpack_ack(dados):
    _, id_req, num_reqs, total_sum = struct.unpack("!BQQQ", dados)
    return id_req, num_reqs, total_sum


# --------- etapa2 -----------
MSG_ORDER_MULTICAST = 10
MSG_RM_ACK = 11
MSG_HEARTBEAT = 12
MSG_ELECTION = 20
MSG_COORDINATOR = 21

# logs
COLOR_RESET = "\033[0m"
COLOR_FE = "\033[96m"        # ciano (Front-End)
COLOR_RM = "\033[92m"        # verde (Replica Manager)
COLOR_ELECTION = "\033[95m"  # magenta (Eleicao)
COLOR_LEADER = "\033[93m"    # amarelo (Novo Lider)
COLOR_FAIL = "\033[91m"      # vermelho (Falhas/Timeouts)

def colorir(texto, cor):
    return f"{cor}{texto}{COLOR_RESET}"

# 1 byte tipo, 8 bytes seq_id, 8 bytes id_req, 8 bytes valor
def pack_order_multicast(seq_id, id_req, valor):
    return struct.pack("!BQQQ", MSG_ORDER_MULTICAST, seq_id, id_req, valor)

def unpack_order_multicast(dados):
    _, seq_id, id_req, valor = struct.unpack("!BQQQ", dados)
    return seq_id, id_req, valor

# 1 byte tipo, 8 bytes seq_id, 8 bytes id_req, 8 bytes num_reqs, 8 bytes total_sum, 4 bytes rm_id
def pack_rm_ack(seq_id, id_req, num_reqs, total_sum, rm_id):
    return struct.pack("!BQQQQI", MSG_RM_ACK, seq_id, id_req, num_reqs, total_sum, rm_id)

def unpack_rm_ack(dados):
    _, seq_id, id_req, num_reqs, total_sum, rm_id = struct.unpack("!BQQQQI", dados)
    return seq_id, id_req, num_reqs, total_sum, rm_id

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
