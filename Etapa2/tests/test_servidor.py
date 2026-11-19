import os
import sys
import threading
import unittest
from unittest import mock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import servidor
import protocolo
from front_end import FrontEnd
from eleicao_anel import ip_para_id


class SocketCapturador:
    def __init__(self):
        self.enviados = []

    def sendto(self, pacote, destino):
        self.enviados.append((pacote, destino))

    def close(self):
        pass


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

    def test_fe_poda_historico(self):
        sock_clu = SocketCapturador()
        fe_id = ip_para_id("127.0.0.1")
        rm_id = ip_para_id("127.0.0.2")
        fe_addr = ("127.0.0.1", 5201)

        fe = FrontEnd(
            "127.0.0.1",
            fe_id,
            {fe_id: fe_addr, rm_id: ("127.0.0.2", 5201)},
            fe_id,
            SocketCapturador(),
            sock_clu,
            max_historico=5,
        )
        for i in range(1, 10):
            fe.operacoes[i] = {
                "dados_op": (500, i, 10),
                "rms_que_votaram": set(),
                "votos_concordantes": {},
                "resultado_final": None,
                "evento_quorum": threading.Event(),
                "em_andamento": False,
            }
            fe.op_to_seq[(500, i)] = i
            fe.pendentes.add(i)
            fe.next_seq = i + 1
            fe.tratar_rm_ack(protocolo.pack_rm_ack(i, 500, i, i, i * 10, rm_id))

        self.assertLessEqual(len(fe.operacoes), 5)

    def test_promocao_fe_limpa_pendencia_e_fe_vivo_responde_coordinator(self):
        novo_fe = servidor.ServidorReplicado("127.0.0.3", 4107, 5107)
        try:
            antigo_fe_id = ip_para_id("127.0.0.1")
            rm2_id = ip_para_id("127.0.0.2")
            novo_fe.nos_cluster[antigo_fe_id] = ("127.0.0.1", 5107)
            novo_fe.nos_cluster[rm2_id] = ("127.0.0.2", 5107)
            novo_fe.leader_id = antigo_fe_id
            novo_fe.detector.registrar(rm2_id)
            novo_fe.ultimo_seq_no[rm2_id] = 1
            novo_fe.rm.buffer.processar(1, 1001, 1, 10)
            novo_fe.rm.buffer.processar(3, 1002, 1, 99)

            novo_fe.assumir_lideranca()
            self.assertTrue(novo_fe.is_leader())
            self.assertEqual(novo_fe.rm.buffer.pendencias, {})

            sock_fe = SocketCapturador()
            novo_fe.sock_cluster.close()
            novo_fe.sock_cluster = sock_fe
            novo_fe.tratar_eleicao(
                protocolo.pack_election(rm2_id, rm2_id), ("127.0.0.2", 5107)
            )
            self.assertFalse(novo_fe.em_eleicao)
            coords = [p for p, _ in sock_fe.enviados if p[0] == protocolo.MSG_COORDINATOR]
            self.assertEqual(len(coords), 1)
            self.assertEqual(protocolo.unpack_coordinator(coords[0]), novo_fe.node_id)

            sock_cli = SocketCapturador()
            novo_fe.fe.sock_cliente = sock_cli
            novo_fe.fe.rms_ativos.clear()
            novo_fe.fe.processar_requisicao_cliente(
                protocolo.pack_req(2002, 1, 40), ("127.0.0.10", 6001), lambda: True
            )
            self.assertEqual(len(sock_cli.enviados), 1)
            self.assertEqual(
                protocolo.unpack_ack(sock_cli.enviados[0][0]), (2002, 1, 2, 50)
            )
        finally:
            novo_fe.fechar_sockets()

    def test_obter_estado_sincronizacao_com_pendencias(self):
        fe_id = ip_para_id("127.0.0.1")
        sock_clu = SocketCapturador()
        fe = FrontEnd(
            "127.0.0.1",
            fe_id,
            {fe_id: ("127.0.0.1", 5201)},
            fe_id,
            SocketCapturador(),
            sock_clu,
        )
        fe.operacoes[1] = {
            "dados_op": (10, 1, 50),
            "rms_que_votaram": set(),
            "votos_concordantes": {},
            "resultado_final": (1, 50),
            "evento_quorum": threading.Event(),
            "em_andamento": False,
        }
        fe.operacoes[1]["evento_quorum"].set()
        fe.num_reqs = 1
        fe.total_sum = 50

        fe.operacoes[2] = {
            "dados_op": (10, 2, 30),
            "rms_que_votaram": set(),
            "votos_concordantes": {},
            "resultado_final": None,
            "evento_quorum": threading.Event(),
            "em_andamento": True,
        }
        fe.pendentes.add(2)
        fe.next_seq = 3

        sync_seq, num_reqs, total_sum = fe.obter_estado_sincronizacao()
        self.assertEqual(sync_seq, 2)
        self.assertEqual(num_reqs, 1)
        self.assertEqual(total_sum, 50)

        pacotes = fe.obter_pacotes_pendentes()
        self.assertEqual(len(pacotes), 1)
        seq_p, c_id, r_id, val = protocolo.unpack_order_multicast(pacotes[0])
        self.assertEqual((seq_p, c_id, r_id, val), (2, 10, 2, 30))

    def test_voto_divergente_recebe_state_sync(self):
        fe_id = ip_para_id("127.0.0.1")
        rm1_id = ip_para_id("127.0.0.2")
        rm2_id = ip_para_id("127.0.0.3")
        rm3_id = ip_para_id("127.0.0.4")
        sock_clu = SocketCapturador()

        fe = FrontEnd(
            "127.0.0.1",
            fe_id,
            {
                fe_id: ("127.0.0.1", 5201),
                rm1_id: ("127.0.0.2", 5201),
                rm2_id: ("127.0.0.3", 5201),
                rm3_id: ("127.0.0.4", 5201),
            },
            fe_id,
            SocketCapturador(),
            sock_clu,
        )
        fe.operacoes[1] = {
            "dados_op": (10, 1, 100),
            "rms_que_votaram": set(),
            "votos_concordantes": {},
            "resultado_final": None,
            "evento_quorum": threading.Event(),
            "em_andamento": True,
        }
        fe.pendentes.add(1)
        fe.next_seq = 2

        fe.tratar_rm_ack(protocolo.pack_rm_ack(1, 10, 1, 1, 100, rm1_id))
        fe.tratar_rm_ack(protocolo.pack_rm_ack(1, 10, 1, 0, 50, rm3_id))
        fe.tratar_rm_ack(protocolo.pack_rm_ack(1, 10, 1, 1, 100, rm2_id))

        self.assertTrue(fe.operacoes[1]["evento_quorum"].is_set())
        sync_enviados = [
            (p, dest)
            for p, dest in sock_clu.enviados
            if p[0] == protocolo.MSG_STATE_SYNC
        ]
        self.assertEqual(len(sync_enviados), 1)
        pct, dest = sync_enviados[0]
        self.assertEqual(dest, ("127.0.0.4", 5201))
        n_seq, n_reqs, t_sum = protocolo.unpack_state_sync(pct)
        self.assertEqual((n_seq, n_reqs, t_sum), (2, 1, 100))


if __name__ == "__main__":
    unittest.main()
