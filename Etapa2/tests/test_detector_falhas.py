import os
import sys
import unittest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from detector_falhas import DetectorFalhas


class RelogioFalso:
    def __init__(self):
        self.agora = 0.0

    def __call__(self):
        return self.agora


class TestDetectorFalhas(unittest.TestCase):
    def setUp(self):
        self.relogio = RelogioFalso()
        self.detector = DetectorFalhas(timeout=1.0, relogio=self.relogio)

    def test_primeiro_heartbeat_indica_ingresso(self):
        self.assertEqual(self.detector.registrar(10), "ingresso")
        self.assertEqual(self.detector.ativos(), {10})

    def test_heartbeats_seguintes_nao_geram_evento(self):
        self.detector.registrar(10)
        self.relogio.agora = 0.5
        self.assertIsNone(self.detector.registrar(10))

    def test_no_dentro_do_timeout_continua_ativo(self):
        self.detector.registrar(10)
        self.relogio.agora = 1.0
        self.assertEqual(self.detector.verificar(), [])
        self.assertEqual(self.detector.ativos(), {10})

    def test_no_silencioso_alem_do_timeout_falha(self):
        self.detector.registrar(10)
        self.detector.registrar(20)
        self.relogio.agora = 0.8
        self.detector.registrar(20)
        self.relogio.agora = 1.5
        self.assertEqual(self.detector.verificar(), [10])
        self.assertEqual(self.detector.ativos(), {20})

    def test_falha_e_informada_uma_unica_vez(self):
        self.detector.registrar(10)
        self.relogio.agora = 2.0
        self.assertEqual(self.detector.verificar(), [10])
        self.assertEqual(self.detector.verificar(), [])

    def test_no_que_volta_apos_falha_indica_retorno(self):
        self.detector.registrar(10)
        self.relogio.agora = 2.0
        self.detector.verificar()
        self.assertEqual(self.detector.registrar(10), "retorno")
        self.assertEqual(self.detector.ativos(), {10})

    def test_heartbeat_renovado_adia_a_falha(self):
        self.detector.registrar(10)
        self.relogio.agora = 0.9
        self.detector.registrar(10)
        self.relogio.agora = 1.8
        self.assertEqual(self.detector.verificar(), [])


if __name__ == "__main__":
    unittest.main()
