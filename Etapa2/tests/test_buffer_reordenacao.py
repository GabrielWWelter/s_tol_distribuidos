import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from buffer_reordenacao import BufferReordenacao

class TestBufferReordenacao(unittest.TestCase):
    def setUp(self):
        self.buf = BufferReordenacao()

    def test_processamento_em_ordem(self):
        ok, aplicadas = self.buf.processar(seq_id=1, id_req=1, valor=10)
        self.assertTrue(ok)
        self.assertEqual(len(aplicadas), 1)
        self.assertEqual(aplicadas[0], (1, 1, 1, 10))

        ok, aplicadas = self.buf.processar(seq_id=2, id_req=2, valor=20)
        self.assertTrue(ok)
        self.assertEqual(aplicadas[0], (2, 2, 2, 30))
        self.assertEqual(self.buf.total_sum, 30)
        self.assertEqual(self.buf.num_reqs, 2)

    def test_processamento_fora_de_ordem(self):
        # chega seq 3 primeiro (armazena no buffer)
        ok, aplicadas = self.buf.processar(seq_id=3, id_req=3, valor=30)
        self.assertFalse(ok)
        self.assertEqual(aplicadas, [])
        self.assertEqual(self.buf.total_sum, 0)
        self.assertEqual(self.buf.num_reqs, 0)

        # chega seq 1 (processa seq 1)
        ok, aplicadas = self.buf.processar(seq_id=1, id_req=1, valor=10)
        self.assertTrue(ok)
        self.assertEqual(len(aplicadas), 1)
        self.assertEqual(aplicadas[0], (1, 1, 1, 10))

        # chega seq 2 (processa seq 2 e desencadeia seq 3 que estava pendente)
        ok, aplicadas = self.buf.processar(seq_id=2, id_req=2, valor=20)
        self.assertTrue(ok)
        self.assertEqual(len(aplicadas), 2)
        self.assertEqual(aplicadas[0], (2, 2, 2, 30))
        self.assertEqual(aplicadas[1], (3, 3, 3, 60))
        self.assertEqual(self.buf.total_sum, 60)
        self.assertEqual(self.buf.num_reqs, 3)
        self.assertEqual(self.buf.expected_seq, 4)

    def test_descarte_duplicados(self):
        self.buf.processar(seq_id=1, id_req=1, valor=10)
        self.buf.processar(seq_id=2, id_req=2, valor=20)

        # reenvio do seq 1 (duplicado)
        ok, aplicadas = self.buf.processar(seq_id=1, id_req=1, valor=10)
        self.assertFalse(ok)
        self.assertEqual(len(aplicadas), 1)
        # retorna ultimo estado sem somar novamente
        self.assertEqual(aplicadas[0], (1, 1, 2, 30))
        self.assertEqual(self.buf.total_sum, 30)
        self.assertEqual(self.buf.num_reqs, 2)

    def test_mascara_64bits(self):
        self.buf.processar(seq_id=1, id_req=1, valor=2**64 - 1)
        self.assertEqual(self.buf.total_sum, 2**64 - 1)

        # overflow para 0
        self.buf.processar(seq_id=2, id_req=2, valor=1)
        self.assertEqual(self.buf.total_sum, 0)

if __name__ == '__main__':
    unittest.main()
