import os
import sys
import unittest
from unittest import mock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import servidor
import protocolo
from eleicao_anel import ip_para_id


class TestServidorDinamico(unittest.TestCase):
    def test_obter_ip_interface(self):
        ip = servidor.obter_ip_interface()
        self.assertIsInstance(ip, str)
        partes = ip.split(".")
        self.assertEqual(len(partes), 4)

    def test_alocar_ip_servidor_multiplas_instancias_mesma_maquina(self):
        porta_teste = 56123
        alocados = []
        try:
            for _ in range(3):
                ip, sock_res = servidor.alocar_ip_servidor(porta_teste)
                alocados.append((ip, sock_res))
            ips = [ip for ip, _ in alocados]
            # Todos os 3 servidores na mesma máquina devem receber IPs distintos automaticamente
            self.assertEqual(len(set(ips)), 3)
        finally:
            for _, s in alocados:
                s.close()

    def test_servidor_1_se_autoproclama_fe(self):
        srv = servidor.ServidorReplicado("127.0.0.1", 4101, 5101)
        try:
            with mock.patch.object(srv.evento_descoberta, "wait", return_value=False):
                srv.descobrir_cluster_inicial()

            self.assertTrue(srv.is_leader())
            self.assertEqual(srv.leader_id, srv.node_id)
            self.assertEqual(srv.fe.rms_ativos, set())
        finally:
            srv.fechar_sockets()

    def test_fe_recebe_join_e_adiciona_rm(self):
        fe_srv = servidor.ServidorReplicado("127.0.0.1", 4102, 5102)
        try:
            fe_srv.assumir_lideranca_inicial()
            self.assertTrue(fe_srv.is_leader())

            rm_id = ip_para_id("127.0.0.2")
            pct_join = protocolo.pack_rm_join(rm_id, 5103)
            fe_srv.tratar_rm_join(pct_join, ("127.0.0.2", 5103))

            self.assertIn(rm_id, fe_srv.fe.rms_ativos)
            self.assertIn(rm_id, fe_srv.nos_cluster)
            self.assertEqual(fe_srv.nos_cluster[rm_id], ("127.0.0.2", 5103))
        finally:
            fe_srv.fechar_sockets()

    def test_rm_atualiza_topologia_e_remove_inativos(self):
        rm_srv = servidor.ServidorReplicado("127.0.0.2", 4103, 5103)
        try:
            fe_id = ip_para_id("127.0.0.1")
            rm3_id = ip_para_id("127.0.0.3")
            pct_topo1 = protocolo.pack_topology(fe_id, ["127.0.0.1", "127.0.0.2", "127.0.0.3"])
            rm_srv.tratar_topologia(pct_topo1)
            self.assertIn(rm3_id, rm_srv.nos_cluster)

            # Após queda do RM 127.0.0.3, nova topologia não o inclui mais
            pct_topo2 = protocolo.pack_topology(fe_id, ["127.0.0.1", "127.0.0.2"])
            rm_srv.tratar_topologia(pct_topo2)
            self.assertNotIn(rm3_id, rm_srv.nos_cluster)
            self.assertEqual(rm_srv.leader_id, fe_id)
        finally:
            rm_srv.fechar_sockets()

    def test_coordenador_com_ip_menor_que_antigo_fe_caido_e_aceito(self):
        rm_srv = servidor.ServidorReplicado("127.0.0.1", 4104, 5104)
        try:
            antigo_fe_id = ip_para_id("192.168.1.50")
            novo_fe_id = ip_para_id("127.0.0.2")
            rm_srv.nos_cluster[antigo_fe_id] = ("192.168.1.50", 5104)
            rm_srv.nos_cluster[novo_fe_id] = ("127.0.0.2", 5104)
            rm_srv.leader_id = antigo_fe_id
            rm_srv.detector.registrar(antigo_fe_id)

            pct_coord = protocolo.pack_coordinator(novo_fe_id)
            rm_srv.tratar_controle(protocolo.MSG_COORDINATOR, pct_coord, ("127.0.0.2", 5104))
            self.assertEqual(rm_srv.leader_id, novo_fe_id)
        finally:
            rm_srv.fechar_sockets()


if __name__ == "__main__":
    unittest.main()
