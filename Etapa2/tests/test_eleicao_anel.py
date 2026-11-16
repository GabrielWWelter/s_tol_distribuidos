import os
import sys
import unittest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from eleicao_anel import ip_para_id, sucessor, tratar_eleicao


def eleger(ids, iniciador, inativos=()):
    participantes = {iniciador}
    candidato = iniciador
    atual = sucessor(ids, iniciador, inativos)
    if atual is None:
        return iniciador
    for _ in range(3 * len(ids)):
        acao, candidato = tratar_eleicao(atual, candidato, atual in participantes)
        if acao == "eleito":
            return candidato
        participantes.add(atual)
        atual = sucessor(ids, atual, inativos)
    raise AssertionError("eleição não convergiu")


class TestIdentificador(unittest.TestCase):
    def test_ip_maior_gera_id_maior(self):
        self.assertLess(ip_para_id("10.0.0.2"), ip_para_id("10.0.0.10"))
        self.assertLess(ip_para_id("10.255.255.255"), ip_para_id("192.168.0.1"))

    def test_id_cabe_em_32_bits(self):
        self.assertEqual(ip_para_id("255.255.255.255"), 0xFFFFFFFF)


class TestSucessor(unittest.TestCase):
    ids = [10, 30, 20, 40]

    def test_anel_segue_ordem_crescente_e_volta_ao_inicio(self):
        self.assertEqual(sucessor(self.ids, 10), 20)
        self.assertEqual(sucessor(self.ids, 40), 10)

    def test_pula_sucessor_inativo(self):
        self.assertEqual(sucessor(self.ids, 10, inativos={20}), 30)

    def test_pula_varios_inativos_seguidos(self):
        self.assertEqual(sucessor(self.ids, 30, inativos={40, 10}), 20)

    def test_sem_sucessor_ativo_retorna_none(self):
        self.assertIsNone(sucessor(self.ids, 10, inativos={20, 30, 40}))


class TestTratarEleicao(unittest.TestCase):
    def test_repassa_candidato_maior(self):
        self.assertEqual(tratar_eleicao(20, 30, False), ("repassar", 30))

    def test_troca_candidato_menor_pelo_proprio_id(self):
        self.assertEqual(tratar_eleicao(20, 10, False), ("repassar", 20))

    def test_participante_descarta_candidato_menor(self):
        self.assertEqual(tratar_eleicao(20, 10, True), ("descartar", None))

    def test_proprio_id_de_volta_elege(self):
        self.assertEqual(tratar_eleicao(20, 20, True), ("eleito", 20))


class TestConvergencia(unittest.TestCase):
    ids = [ip_para_id(f"10.0.0.{n}") for n in (1, 2, 3, 4, 5)]

    def test_elege_maior_id_de_qualquer_iniciador(self):
        for iniciador in self.ids:
            self.assertEqual(eleger(self.ids, iniciador), max(self.ids))

    def test_queda_do_lider_elege_maior_ativo(self):
        lider = max(self.ids)
        self.assertEqual(eleger(self.ids, self.ids[0], {lider}), self.ids[3])

    def test_quedas_sucessivas_elegem_maior_restante(self):
        inativos = {self.ids[4], self.ids[3], self.ids[1]}
        self.assertEqual(eleger(self.ids, self.ids[0], inativos), self.ids[2])

    def test_unico_ativo_se_elege(self):
        self.assertEqual(eleger(self.ids, self.ids[0], set(self.ids[1:])), self.ids[0])


if __name__ == "__main__":
    unittest.main()
