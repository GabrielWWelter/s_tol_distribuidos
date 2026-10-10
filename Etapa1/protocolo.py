import struct

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
