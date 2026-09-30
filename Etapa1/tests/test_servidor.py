import unittest
import socket
import subprocess
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import protocolo

PORTA_TESTE = 4999

class TestServidor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        servidor_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'servidor.py'))
        cls.processo = subprocess.Popen(
            [sys.executable, servidor_path, str(PORTA_TESTE)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(0.3)
        cls.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cls.sock.settimeout(1.0)
        cls.server_addr = ('127.0.0.1', PORTA_TESTE)

    @classmethod
    def tearDownClass(cls):
        cls.sock.close()
        cls.processo.terminate()
        cls.processo.wait()

    def test_1_descoberta(self):
        msg = protocolo.pack_discovery()
        self.sock.sendto(msg, self.server_addr)
        resp, _ = self.sock.recvfrom(1024)
        self.assertEqual(resp[0], protocolo.MSG_DISCOVERY_RESP)

    def test_2_requisicao_em_ordem(self):
        # Primeira requisicao: 10
        req1 = protocolo.pack_req(1, 10)
        self.sock.sendto(req1, self.server_addr)
        resp, _ = self.sock.recvfrom(1024)
        id_req, num_reqs, total_sum = protocolo.unpack_ack(resp)
        self.assertEqual(id_req, 1)
        self.assertEqual(num_reqs, 1)
        self.assertEqual(total_sum, 10)

        # Segunda requisicao: 20
        req2 = protocolo.pack_req(2, 20)
        self.sock.sendto(req2, self.server_addr)
        resp, _ = self.sock.recvfrom(1024)
        id_req, num_reqs, total_sum = protocolo.unpack_ack(resp)
        self.assertEqual(id_req, 2)
        self.assertEqual(num_reqs, 2)
        self.assertEqual(total_sum, 30)

    def test_3_requisicao_duplicada(self):
        # Reenvia requisicao 2: nao deve somar de novo
        req2 = protocolo.pack_req(2, 20)
        self.sock.sendto(req2, self.server_addr)
        resp, _ = self.sock.recvfrom(1024)
        id_req, num_reqs, total_sum = protocolo.unpack_ack(resp)
        self.assertEqual(id_req, 2)
        self.assertEqual(num_reqs, 2)
        self.assertEqual(total_sum, 30)

    def test_4_salto_de_requisicao(self):
        # Envia requisicao 5 (pulou 3 e 4): servidor responde com ultimo id valido (2)
        req5 = protocolo.pack_req(5, 50)
        self.sock.sendto(req5, self.server_addr)
        resp, _ = self.sock.recvfrom(1024)
        id_req, num_reqs, total_sum = protocolo.unpack_ack(resp)
        self.assertEqual(id_req, 2)
        self.assertEqual(num_reqs, 2)
        self.assertEqual(total_sum, 30)

if __name__ == '__main__':
    unittest.main()
