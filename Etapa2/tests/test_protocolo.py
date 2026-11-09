import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import protocolo

class TestProtocoloEtapa2(unittest.TestCase):
    def test_descoberta(self):
        msg = protocolo.pack_discovery()
        self.assertEqual(msg[0], protocolo.MSG_DISCOVERY)

        resp = protocolo.pack_discovery_resp()
        self.assertEqual(resp[0], protocolo.MSG_DISCOVERY_RESP)

    def test_requisicao_e_ack(self):
        id_req = 1
        valor = 500
        req = protocolo.pack_req(id_req, valor)
        unpacked_id, unpacked_valor = protocolo.unpack_req(req)
        self.assertEqual(unpacked_id, id_req)
        self.assertEqual(unpacked_valor, valor)

        num_reqs = 10
        total_sum = 2500
        ack = protocolo.pack_ack(id_req, num_reqs, total_sum)
        unpacked_id, unpacked_num, unpacked_sum = protocolo.unpack_ack(ack)
        self.assertEqual(unpacked_id, id_req)
        self.assertEqual(unpacked_num, num_reqs)
        self.assertEqual(unpacked_sum, total_sum)

    def test_order_multicast(self):
        seq_id = 42
        id_req = 7
        valor = 12345
        pacote = protocolo.pack_order_multicast(seq_id, id_req, valor)

        unpacked_seq, unpacked_id, unpacked_valor = protocolo.unpack_order_multicast(pacote)
        self.assertEqual(unpacked_seq, seq_id)
        self.assertEqual(unpacked_id, id_req)
        self.assertEqual(unpacked_valor, valor)

    def test_rm_ack(self):
        seq_id = 42
        id_req = 7
        num_reqs = 15
        total_sum = 99999
        rm_id = 3
        pacote = protocolo.pack_rm_ack(seq_id, id_req, num_reqs, total_sum, rm_id)

        u_seq, u_id, u_num, u_sum, u_rm = protocolo.unpack_rm_ack(pacote)
        self.assertEqual(u_seq, seq_id)
        self.assertEqual(u_id, id_req)
        self.assertEqual(u_num, num_reqs)
        self.assertEqual(u_sum, total_sum)
        self.assertEqual(u_rm, rm_id)

    def test_heartbeat(self):
        leader_id = 1
        last_seq = 100
        pacote = protocolo.pack_heartbeat(leader_id, last_seq)

        u_leader, u_last_seq = protocolo.unpack_heartbeat(pacote)
        self.assertEqual(u_leader, leader_id)
        self.assertEqual(u_last_seq, last_seq)

    def test_eleicao_e_coordenador(self):
        sender_id = 2
        highest_id = 4
        pacote = protocolo.pack_election(sender_id, highest_id)

        u_sender, u_highest = protocolo.unpack_election(pacote)
        self.assertEqual(u_sender, sender_id)
        self.assertEqual(u_highest, highest_id)

        coord = protocolo.pack_coordinator(highest_id)
        u_leader = protocolo.unpack_coordinator(coord)
        self.assertEqual(u_leader, highest_id)

    def test_cores_ansi(self):
        texto = protocolo.colorir("TESTE", protocolo.COLOR_FE)
        self.assertTrue(texto.startswith(protocolo.COLOR_FE))
        self.assertTrue(texto.endswith(protocolo.COLOR_RESET))

if __name__ == '__main__':
    unittest.main()
