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
            # Sem RMs ativos no cluster, o FE soma sozinho e responde
            self.assertEqual(len(sock_cli.enviados), 1)
            self.assertEqual(
                protocolo.unpack_ack(sock_cli.enviados[0][0]), (2002, 1, 2, 50)
            )

            # Reenvio do cliente devolve o mesmo resultado, sem somar de novo
            novo_fe.fe.processar_requisicao_cliente(
                protocolo.pack_req(2002, 1, 40), ("127.0.0.10", 6001), lambda: True
            )
            self.assertEqual(protocolo.unpack_ack(sock_cli.enviados[1][0]), (2002, 1, 2, 50))
            self.assertEqual(novo_fe.fe.obter_estado_sincronizacao(), (3, 2, 50))
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

    def test_fe_eleito_atrasado_nao_envia_estado_errado(self):
        novo_fe = servidor.ServidorReplicado("127.0.0.3", 4120, 5120)
        try:
            antigo_id = ip_para_id("127.0.0.4")
            b_id = ip_para_id("127.0.0.2")
            c_id = ip_para_id("127.0.0.1")
            for no_id, ip in ((antigo_id, "127.0.0.4"), (b_id, "127.0.0.2"), (c_id, "127.0.0.1")):
                novo_fe.nos_cluster[no_id] = (ip, 5120)
            novo_fe.leader_id = antigo_id
            # A (novo FE) e C aplicaram só a op 1 (10); B já aplicou a op 2 (90)
            novo_fe.rm.buffer.processar(1, 900, 1, 10)
            novo_fe.detector.registrar(b_id)
            novo_fe.detector.registrar(c_id)
            novo_fe.ultimo_seq_no[b_id] = 2
            novo_fe.ultimo_seq_no[c_id] = 1

            novo_fe.assumir_lideranca()
            sock = SocketCapturador()
            novo_fe.sock_cluster.close()
            novo_fe.sock_cluster = sock
            novo_fe.fe.sock_cluster = sock
            self.assertEqual(novo_fe.fe.next_seq, 3)
            self.assertEqual(novo_fe.fe.rms_ativos, {b_id})

            # C tenta reintegrar antes da 1ª confirmação: nada de STATE_SYNC (1, 10)
            novo_fe._reintegrar_rm(c_id, "127.0.0.1", 5120, forcar_sync=True)
            syncs = [p for p, _ in sock.enviados if p[0] == protocolo.MSG_STATE_SYNC]
            self.assertEqual(syncs, [])
            self.assertNotIn(c_id, novo_fe.fe.rms_ativos)

            # B confirma a op 3 (valor 5): totais certos chegam ao FE
            with novo_fe.fe.lock_fe:
                novo_fe.fe.operacoes[3] = {
                    "dados_op": (901, 1, 5), "rms_que_votaram": set(),
                    "votos_concordantes": {}, "resultado_final": None,
                    "evento_quorum": threading.Event(), "em_andamento": False,
                }
                novo_fe.fe.op_to_seq[(901, 1)] = 3
                novo_fe.fe.pendentes.add(3)
                novo_fe.fe.next_seq = 4
            novo_fe.fe.tratar_rm_ack(protocolo.pack_rm_ack(3, 901, 1, 3, 105, b_id))
            self.assertIsNone(novo_fe.fe.seq_sem_estado)

            novo_fe._reintegrar_rm(c_id, "127.0.0.1", 5120, forcar_sync=True)
            syncs = [p for p, _ in sock.enviados if p[0] == protocolo.MSG_STATE_SYNC]
            self.assertEqual(protocolo.unpack_state_sync(syncs[-1]), (4, 3, 105))
        finally:
            novo_fe.fechar_sockets()

    def test_fe_sozinho_resolve_pendencias_quando_ultimo_rm_cai(self):
        fe_id = ip_para_id("127.0.0.1")
        rm_id = ip_para_id("127.0.0.2")
        sock_clu = SocketCapturador()
        sock_cli = SocketCapturador()
        nos = {fe_id: ("127.0.0.1", 5000), rm_id: ("127.0.0.2", 5000)}
        fe = FrontEnd("127.0.0.1", fe_id, nos, fe_id, sock_cli, sock_clu)
        fe.ativar(1, {}, {rm_id})
        # RM confirma a op 1 (10); a op 2 (5) fica pendente quando o RM cai
        for seq, req, val in ((1, 1, 10), (2, 2, 5)):
            with fe.lock_fe:
                fe.operacoes[seq] = {
                    "dados_op": (700, req, val), "rms_que_votaram": set(),
                    "votos_concordantes": {}, "resultado_final": None,
                    "evento_quorum": threading.Event(), "em_andamento": False,
                }
                fe.op_to_seq[(700, req)] = seq
                fe.pendentes.add(seq)
                fe.next_seq = seq + 1
        fe.tratar_rm_ack(protocolo.pack_rm_ack(1, 700, 1, 1, 10, rm_id))
        fe.marcar_rm(rm_id, False)
        self.assertEqual(fe.operacoes[2]["resultado_final"], (2, 15))
        self.assertEqual(fe.obter_estado_sincronizacao(), (3, 2, 15))

        # Nova requisição com o FE sozinho continua a sequência
        fe.processar_requisicao_cliente(protocolo.pack_req(701, 1, 7), ("127.0.0.9", 6000), lambda: True)
        self.assertEqual(protocolo.unpack_ack(sock_cli.enviados[-1][0]), (701, 1, 3, 22))

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

    def test_rm_trata_duplicata_resequenciada_apos_failover(self):
        fe_id = ip_para_id("127.0.0.1")
        rm_id = ip_para_id("127.0.0.2")
        sock_clu = SocketCapturador()
        from replica_manager import ReplicaManager
        rm = ReplicaManager("127.0.0.2", rm_id, {fe_id: ("127.0.0.1", 5201)}, sock_clu)

        # RM processa seq=1 do líder original
        rm.tratar_order_multicast(protocolo.pack_order_multicast(1, 1001, 1, 15), ("127.0.0.1", 5201), fe_id)
        self.assertEqual(rm.buffer.expected_seq, 2)
        self.assertEqual(rm.buffer.total_sum, 15)

        # Apos failover, novo líder reenvia a mesma operacao sob seq=2
        rm.tratar_order_multicast(protocolo.pack_order_multicast(2, 1001, 1, 15), ("127.0.0.1", 5201), fe_id)

        # RM avanca para seq=3 sem duplicar total_sum e envia RM_ACK para o novo FE
        self.assertEqual(rm.buffer.expected_seq, 3)
        self.assertEqual(rm.buffer.total_sum, 15)
        self.assertEqual(rm.buffer.num_reqs, 1)
        acks = [p for p, _ in sock_clu.enviados if p[0] == protocolo.MSG_RM_ACK]
        self.assertEqual(len(acks), 2)

    def test_reconexao_cabo_rm_isolado_reverte_split_brain_sem_congelar_lider(self):
        fe_real = servidor.ServidorReplicado("127.0.0.203", 4120, 5120)
        rm_ativo = servidor.ServidorReplicado("127.0.0.122", 4121, 5120)
        rm_isolado = servidor.ServidorReplicado("127.0.0.94", 4122, 5120)
        try:
            fe_real.assumir_lideranca_inicial()
            fe_real.fe.next_seq = 500
            fe_real.fe.num_reqs = 499
            fe_real.fe.total_sum = 5000
            fe_real.fe.adicionar_rm(rm_ativo.node_id, "127.0.0.122", 5120)
            fe_real.nos_cluster[rm_ativo.node_id] = ("127.0.0.122", 5120)
            fe_real.detector.registrar(rm_ativo.node_id)

            rm_ativo.nos_cluster[fe_real.node_id] = ("127.0.0.203", 5120)
            rm_ativo.leader_id = fe_real.node_id
            rm_ativo.detector.registrar(fe_real.node_id)
            rm_ativo.rm.sincronizar(500, 499, 5000)

            rm_isolado.rm.sincronizar(100, 99, 1000)
            rm_isolado.assumir_lideranca()
            self.assertTrue(rm_isolado.is_leader())

            sock_fe = SocketCapturador()
            sock_isolado = SocketCapturador()
            fe_real.sock_cluster.close()
            fe_real.sock_cluster = sock_fe
            fe_real.fe.sock_cluster = sock_fe
            rm_isolado.sock_cluster.close()
            rm_isolado.sock_cluster = sock_isolado
            rm_isolado.fe.sock_cluster = sock_isolado
            rm_isolado.rm.sock_cluster = sock_isolado

            topo_falsa = protocolo.pack_topology(
                rm_isolado.node_id, ["127.0.0.94", "127.0.0.122", "127.0.0.203"]
            )
            fe_real.ultimo_seq_no[rm_isolado.node_id] = 99
            rm_ativo.ultimo_seq_no[rm_isolado.node_id] = 99
            fe_real.tratar_topologia(topo_falsa, ("127.0.0.94", 5120))
            rm_ativo.tratar_topologia(topo_falsa, ("127.0.0.94", 5120))

            self.assertTrue(fe_real.is_leader())
            self.assertEqual(rm_ativo.leader_id, fe_real.node_id)

            hb_fe_real = protocolo.pack_heartbeat(fe_real.node_id, 499)
            rm_isolado.tratar_heartbeat(hb_fe_real, ("127.0.0.203", 5120))

            self.assertFalse(rm_isolado.is_leader())
            self.assertEqual(rm_isolado.leader_id, fe_real.node_id)
            topos_indevidas = [
                p for p, _ in sock_isolado.enviados if p[0] == protocolo.MSG_TOPOLOGY
            ]
            self.assertEqual(topos_indevidas, [])
            joins = [
                (p, d) for p, d in sock_isolado.enviados if p[0] == protocolo.MSG_RM_JOIN
            ]
            self.assertEqual(len(joins), 1)

            fe_real.tratar_rm_join(joins[0][0], ("127.0.0.94", 5120))
            self.assertIn(rm_isolado.node_id, fe_real.fe.rms_ativos)
            syncs = [
                p for p, d in sock_fe.enviados
                if p[0] == protocolo.MSG_STATE_SYNC and d[0] == "127.0.0.94"
            ]
            self.assertEqual(len(syncs), 1)
            n_seq, n_reqs, t_sum = protocolo.unpack_state_sync(syncs[0])
            rm_isolado.rm.sincronizar(n_seq, n_reqs, t_sum, fe_real.node_id)
            self.assertEqual(rm_isolado.rm.buffer.expected_seq, 500)
            self.assertEqual(rm_isolado.rm.buffer.total_sum, 5000)
        finally:
            fe_real.fechar_sockets()
            rm_ativo.fechar_sockets()
            rm_isolado.fechar_sockets()

    def test_rm_atrasado_reintegra_atomicamente_via_heartbeat_sem_loop_infinito(self):
        fe_srv = servidor.ServidorReplicado("127.0.0.203", 4123, 5123)
        try:
            fe_srv.assumir_lideranca_inicial()
            fe_srv.fe.next_seq = 500
            fe_srv.fe.num_reqs = 499
            fe_srv.fe.total_sum = 9999

            sock_fe = SocketCapturador()
            fe_srv.sock_cluster.close()
            fe_srv.sock_cluster = sock_fe
            fe_srv.fe.sock_cluster = sock_fe

            rm_id = ip_para_id("127.0.0.94")
            hb_atrasado = protocolo.pack_heartbeat(fe_srv.node_id, 100)
            fe_srv.tratar_heartbeat(hb_atrasado, ("127.0.0.94", 5123))

            self.assertIn(rm_id, fe_srv.fe.rms_ativos)
            syncs = [p for p, _ in sock_fe.enviados if p[0] == protocolo.MSG_STATE_SYNC]
            self.assertEqual(len(syncs), 1)
            self.assertEqual(protocolo.unpack_state_sync(syncs[0]), (500, 499, 9999))
        finally:
            fe_srv.fechar_sockets()

    def test_antigo_fe_isolado_cede_lideranca_ao_reconectar_com_cluster_avancado(self):
        antigo_fe = servidor.ServidorReplicado("127.0.0.203", 4124, 5124)
        novo_fe_id = ip_para_id("127.0.0.122")
        try:
            antigo_fe.assumir_lideranca_inicial()
            antigo_fe.fe.next_seq = 101
            antigo_fe.fe.num_reqs = 100
            antigo_fe.fe.total_sum = 2500

            sock_antigo = SocketCapturador()
            antigo_fe.sock_cluster.close()
            antigo_fe.sock_cluster = sock_antigo
            antigo_fe.fe.sock_cluster = sock_antigo

            hb_novo_fe = protocolo.pack_heartbeat(novo_fe_id, 250)
            antigo_fe.tratar_heartbeat(hb_novo_fe, ("127.0.0.122", 5124))

            self.assertFalse(antigo_fe.is_leader())
            self.assertEqual(antigo_fe.leader_id, novo_fe_id)
            self.assertEqual(antigo_fe.rm.buffer.expected_seq, 101)
            joins = [p for p, _ in sock_antigo.enviados if p[0] == protocolo.MSG_RM_JOIN]
            self.assertEqual(len(joins), 1)
        finally:
            antigo_fe.fechar_sockets()


if __name__ == "__main__":
    unittest.main()
