import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import protocolo

class TestProtocolo(unittest.TestCase):
    def test_descoberta(self):
        msg = protocolo.pack_discovery()
        self.assertEqual(msg[0], protocolo.MSG_DISCOVERY)

        resp = protocolo.pack_discovery_resp()
        self.assertEqual(resp[0], protocolo.MSG_DISCOVERY_RESP)

    def test_requisicao(self):
        id_req = 1
        valor = 100
        pacote = protocolo.pack_req(id_req, valor)

        unpacked_id, unpacked_valor = protocolo.unpack_req(pacote)
        self.assertEqual(unpacked_id, id_req)
        self.assertEqual(unpacked_valor, valor)

    def test_requisicao_uint64(self):
        id_req = 1000000
        valor = 2**64 - 1
        pacote = protocolo.pack_req(id_req, valor)

        unpacked_id, unpacked_valor = protocolo.unpack_req(pacote)
        self.assertEqual(unpacked_valor, valor)

    def test_ack(self):
        id_req = 5
        num_reqs = 10
        total_sum = 500
        pacote = protocolo.pack_ack(id_req, num_reqs, total_sum)

        unpacked_id, unpacked_num, unpacked_sum = protocolo.unpack_ack(pacote)
        self.assertEqual(unpacked_id, id_req)
        self.assertEqual(unpacked_num, num_reqs)
        self.assertEqual(unpacked_sum, total_sum)

if __name__ == '__main__':
    unittest.main()
