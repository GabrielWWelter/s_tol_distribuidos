import os
import sys
import unittest
from unittest import mock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import cliente
import protocolo


class SocketFalso:
    def __init__(self, respostas):
        self.respostas = iter(respostas)
        self.enviados = []
        self.timeout = None

    def settimeout(self, timeout):
        self.timeout = timeout

    def sendto(self, pacote, destino):
        self.enviados.append((pacote, destino))

    def recvfrom(self, _):
        return next(self.respostas), ("127.0.0.1", 4000)


class TestClienteEtapa2(unittest.TestCase):
    def test_client_id_gerado_e_uint64_nao_nulo(self):
        with mock.patch("cliente.secrets.randbits", side_effect=[0, 1234]):
            client_id = cliente.gerar_client_id()

        self.assertEqual(client_id, 1234)
        self.assertLessEqual(client_id, 0xFFFFFFFFFFFFFFFF)

    def test_processamento_reutiliza_stop_and_wait_com_client_id(self):
        client_id = 1001
        id_req = 7
        valor = 50
        destino = ("127.0.0.1", 4000)
        ack_de_outro_cliente = protocolo.pack_ack(9999, id_req, 1, 50)
        ack_correto = protocolo.pack_ack(client_id, id_req, 4, 125)
        sock = SocketFalso([ack_de_outro_cliente, ack_correto])

        with mock.patch("cliente.interface_resposta_recebida") as interface:
            cliente.subservico_processamento(
                sock, destino, client_id, id_req, valor, destino[0]
            )

        self.assertEqual(sock.timeout, cliente.TIMEOUT)
        self.assertEqual(len(sock.enviados), 1)
        pacote, pacote_destino = sock.enviados[0]
        self.assertEqual(pacote_destino, destino)
        self.assertEqual(
            protocolo.unpack_req(pacote), (client_id, id_req, valor)
        )
        interface.assert_called_once_with(destino[0], id_req, valor, 4, 125)


if __name__ == "__main__":
    unittest.main()
