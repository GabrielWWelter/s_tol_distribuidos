import os
import struct
import sys
import unittest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import protocolo


class TestProtocoloEtapa2(unittest.TestCase):
    def test_descoberta(self):
        msg = protocolo.pack_discovery()
        self.assertEqual(msg[0], protocolo.MSG_DISCOVERY)

        resp = protocolo.pack_discovery_resp()
        self.assertEqual(resp[0], protocolo.MSG_DISCOVERY_RESP)

    def test_requisicao_e_ack_incluem_identidade_do_cliente(self):
        client_id = 0x123456789ABCDEF0
        id_req = 1
        valor = 500
        req = protocolo.pack_req(client_id, id_req, valor)
        self.assertEqual(
            protocolo.unpack_req(req), (client_id, id_req, valor)
        )

        num_reqs = 10
        total_sum = 2500
        ack = protocolo.pack_ack(client_id, id_req, num_reqs, total_sum)
        self.assertEqual(
            protocolo.unpack_ack(ack),
            (client_id, id_req, num_reqs, total_sum),
        )

    def test_order_multicast_inclui_identidade_da_operacao(self):
        seq_id = 42
        client_id = 1001
        id_req = 7
        valor = 12345
        pacote = protocolo.pack_order_multicast(
            seq_id, client_id, id_req, valor
        )

        self.assertEqual(
            protocolo.unpack_order_multicast(pacote),
            (seq_id, client_id, id_req, valor),
        )

    def test_rm_ack_inclui_identidade_da_operacao(self):
        seq_id = 42
        client_id = 1001
        id_req = 7
        num_reqs = 15
        total_sum = 99999
        rm_id = 3
        pacote = protocolo.pack_rm_ack(
            seq_id, client_id, id_req, num_reqs, total_sum, rm_id
        )

        self.assertEqual(
            protocolo.unpack_rm_ack(pacote),
            (seq_id, client_id, id_req, num_reqs, total_sum, rm_id),
        )

    def test_unpack_rejeita_datagrama_com_tamanho_incorreto(self):
        pacote = protocolo.pack_order_multicast(1, 2, 3, 4)
        with self.assertRaises(struct.error):
            protocolo.unpack_order_multicast(pacote[:-1])

    def test_heartbeat(self):
        leader_id = 1
        last_seq = 100
        pacote = protocolo.pack_heartbeat(leader_id, last_seq)

        self.assertEqual(
            protocolo.unpack_heartbeat(pacote), (leader_id, last_seq)
        )

    def test_eleicao_e_coordenador(self):
        sender_id = 2
        highest_id = 4
        pacote = protocolo.pack_election(sender_id, highest_id)

        self.assertEqual(
            protocolo.unpack_election(pacote), (sender_id, highest_id)
        )

        coord = protocolo.pack_coordinator(highest_id)
        self.assertEqual(protocolo.unpack_coordinator(coord), highest_id)

    def test_confirmacao_de_eleicao_identifica_quem_recebeu(self):
        ack = protocolo.pack_election_ack(0xC0A80001)
        self.assertEqual(ack[0], protocolo.MSG_ELECTION_ACK)
        self.assertEqual(protocolo.unpack_election_ack(ack), 0xC0A80001)

    def test_cores_ansi(self):
        texto = protocolo.colorir("TESTE", protocolo.COLOR_FE)
        self.assertTrue(texto.startswith(protocolo.COLOR_FE))
        self.assertTrue(texto.endswith(protocolo.COLOR_RESET))


if __name__ == "__main__":
    unittest.main()
