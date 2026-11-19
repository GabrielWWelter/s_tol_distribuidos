import os
import sys
import unittest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from buffer_reordenacao import (
    BufferReordenacao,
    ConflitoSequenciaError,
    OperacaoJaSequenciadaError,
)


class TestBufferReordenacao(unittest.TestCase):
    def setUp(self):
        self.buf = BufferReordenacao()

    def test_processamento_em_ordem(self):
        ok, aplicadas = self.buf.processar(1, 1001, 1, 10)
        self.assertTrue(ok)
        self.assertEqual(aplicadas, [(1, 1001, 1, 1, 10)])

        ok, aplicadas = self.buf.processar(2, 1001, 2, 20)
        self.assertTrue(ok)
        self.assertEqual(aplicadas, [(2, 1001, 2, 2, 30)])
        self.assertEqual(self.buf.total_sum, 30)
        self.assertEqual(self.buf.num_reqs, 2)

    def test_processamento_fora_de_ordem(self):
        ok, aplicadas = self.buf.processar(3, 1001, 3, 30)
        self.assertFalse(ok)
        self.assertEqual(aplicadas, [])

        ok, aplicadas = self.buf.processar(1, 1001, 1, 10)
        self.assertTrue(ok)
        self.assertEqual(aplicadas, [(1, 1001, 1, 1, 10)])

        ok, aplicadas = self.buf.processar(2, 1001, 2, 20)
        self.assertTrue(ok)
        self.assertEqual(
            aplicadas,
            [(2, 1001, 2, 2, 30), (3, 1001, 3, 3, 60)],
        )
        self.assertEqual(self.buf.total_sum, 60)
        self.assertEqual(self.buf.num_reqs, 3)
        self.assertEqual(self.buf.expected_seq, 4)

    def test_duplicado_aplicado_retorna_resultado_original(self):
        self.buf.processar(1, 1001, 1, 10)
        self.buf.processar(2, 1001, 2, 20)

        ok, aplicadas = self.buf.processar(1, 1001, 1, 10)

        self.assertFalse(ok)
        self.assertEqual(aplicadas, [(1, 1001, 1, 1, 10)])
        self.assertEqual(self.buf.total_sum, 30)
        self.assertEqual(self.buf.num_reqs, 2)

    def test_duplicado_pendente_nao_substitui_conteudo(self):
        self.buf.processar(2, 1001, 2, 20)

        ok, aplicadas = self.buf.processar(2, 1001, 2, 20)

        self.assertFalse(ok)
        self.assertEqual(aplicadas, [])
        self.assertEqual(self.buf.pendencias[2], (1001, 2, 20))

    def test_conflito_em_sequencia_pendente_e_rejeitado(self):
        self.buf.processar(2, 1001, 2, 20)

        with self.assertRaises(ConflitoSequenciaError):
            self.buf.processar(2, 9999, 8, 900)

        self.buf.processar(1, 1001, 1, 10)
        self.assertEqual(self.buf.total_sum, 30)

    def test_conflito_em_sequencia_aplicada_e_rejeitado(self):
        self.buf.processar(1, 1001, 1, 10)

        with self.assertRaises(ConflitoSequenciaError):
            self.buf.processar(1, 1001, 1, 11)

        self.assertEqual(self.buf.total_sum, 10)
        self.assertEqual(self.buf.num_reqs, 1)

    def test_mesma_operacao_nao_pode_receber_duas_sequencias(self):
        self.buf.processar(1, 1001, 1, 10)

        with self.assertRaises(OperacaoJaSequenciadaError):
            self.buf.processar(2, 1001, 1, 10)

        self.assertEqual(self.buf.expected_seq, 2)
        self.assertEqual(self.buf.total_sum, 10)

    def test_mesmo_id_req_de_clientes_diferentes_e_valido(self):
        self.buf.processar(1, 1001, 1, 10)
        self.buf.processar(2, 1002, 1, 20)

        self.assertEqual(self.buf.num_reqs, 2)
        self.assertEqual(self.buf.total_sum, 30)

    def test_mascara_64bits(self):
        self.buf.processar(1, 1001, 1, 2**64 - 1)
        self.assertEqual(self.buf.total_sum, 2**64 - 1)

        self.buf.processar(2, 1001, 2, 1)
        self.assertEqual(self.buf.total_sum, 0)

    def test_seq_zero_e_rejeitada(self):
        with self.assertRaises(ValueError):
            self.buf.processar(0, 1001, 1, 10)

    def test_poda_historico_e_limpeza_pendencias_nao_aplicadas(self):
        buf = BufferReordenacao(max_historico=5)
        for s in range(1, 11):
            buf.processar(s, 1001, s, 10)

        self.assertLessEqual(len(buf.historico), 5)
        ok_old, res_old = buf.processar(1, 1001, 1, 10)
        self.assertFalse(ok_old)
        self.assertEqual(res_old, [])

        # Pendência órfã não impede re-sequenciamento após promoção de líder
        buf.processar(12, 1002, 1, 50)
        buf.limpar_pendencias_nao_aplicadas()
        self.assertEqual(buf.pendencias, {})
        ok, res = buf.processar(11, 1002, 1, 50)
        self.assertTrue(ok)
        self.assertEqual(res, [(11, 1002, 1, 11, 150)])

    def test_sincronizar_estado_reinicia_contadores_e_descarrega_pendencias(self):
        buf = BufferReordenacao()
        # RM recebe pacote 50 antes de sincronizar (fica retido)
        buf.processar(50, 1001, 1, 20)
        self.assertIn(50, buf.pendencias)

        # Sincroniza diretamente para seq=50, num_reqs=49, total_sum=1000
        aplicadas = buf.sincronizar_estado(expected_seq=50, num_reqs=49, total_sum=1000)

        # Como a seq 50 ja estava retida, deve ter sido descarregada imediatamente
        self.assertEqual(len(aplicadas), 1)
        self.assertEqual(aplicadas[0], (50, 1001, 1, 50, 1020))
        self.assertEqual(buf.expected_seq, 51)
        self.assertEqual(buf.num_reqs, 50)
        self.assertEqual(buf.total_sum, 1020)


if __name__ == "__main__":
    unittest.main()
