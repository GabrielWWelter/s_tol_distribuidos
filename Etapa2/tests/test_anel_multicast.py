"""Participante/descarte e <elected> no anel; disseminação por IP multicast."""
import os
import socket
import sys
import unittest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import eleicao_anel
import protocolo
import servidor
from eleicao_anel import ip_para_id
from front_end import FrontEnd

PORTA_CLUSTER = 5400
NO_1, NO_2, NO_3 = "127.0.0.71", "127.0.0.72", "127.0.0.73"


class SocketFalso:
    """Guarda os envios e, se ligado a um servidor, confirma as mensagens do anel."""

    def __init__(self, srv=None, falhar_no_grupo=False):
        self.srv = srv
        self.falhar_no_grupo = falhar_no_grupo
        self.enviados = []

    def sendto(self, pacote, destino):
        if self.falhar_no_grupo and destino[0] == protocolo.GRUPO_MULTICAST:
            raise OSError("rede sem multicast")
        self.enviados.append((pacote, destino))
        anel = (protocolo.MSG_ELECTION, protocolo.MSG_ELECTED)
        if self.srv and pacote[0] in anel:
            if self.srv.esperando_ack_de == ip_para_id(destino[0]):
                self.srv.ack_eleicao.set()

    def do_tipo(self, tipo):
        return [(p, d) for p, d in self.enviados if p[0] == tipo]

    def close(self):
        pass


def criar_servidor(ip, porta_cliente):
    ips = [NO_1, NO_2, NO_3]
    srv = servidor.ServidorReplicado(ip, porta_cliente, PORTA_CLUSTER, ips)
    srv.sock_cluster.close()
    srv.sock_cluster = SocketFalso(srv)
    srv.fe.sock_cluster = srv.rm.sock_cluster = srv.sock_cluster
    srv.detector.timeout = 60
    for outro in ips:
        if outro != ip:
            srv.detector.registrar(ip_para_id(outro))
    return srv


class TestDescarteNoAnel(unittest.TestCase):
    def test_participante_descarta_candidato_menor(self):
        acao, _ = eleicao_anel.tratar_eleicao(20, 10, participante=True)
        self.assertEqual(acao, "descartar")

    def test_nao_participante_substitui_candidato_menor(self):
        self.assertEqual(eleicao_anel.tratar_eleicao(20, 10), ("repassar", 20))

    def test_participante_repassa_maior_e_reconhece_o_proprio_id(self):
        self.assertEqual(
            eleicao_anel.tratar_eleicao(20, 30, participante=True), ("repassar", 30)
        )
        self.assertEqual(
            eleicao_anel.tratar_eleicao(20, 20, participante=True), ("eleito", 20)
        )

    def test_servidor_participante_apenas_confirma_o_recebimento(self):
        srv = criar_servidor(NO_2, 4401)
        self.addCleanup(srv.fechar_sockets)
        srv.em_eleicao = True

        pacote = protocolo.pack_election(ip_para_id(NO_1), ip_para_id(NO_1))
        srv.tratar_eleicao(pacote, (NO_1, PORTA_CLUSTER))

        self.assertEqual(len(srv.sock_cluster.do_tipo(protocolo.MSG_ELECTION_ACK)), 1)
        self.assertEqual(srv.sock_cluster.do_tipo(protocolo.MSG_ELECTION), [])


class TestMensagemEleito(unittest.TestCase):
    def setUp(self):
        self.srv = criar_servidor(NO_1, 4402)
        self.addCleanup(self.srv.fechar_sockets)
        self.srv.em_eleicao = True

    def receber_eleito(self, eleito):
        pacote = protocolo.pack_elected(ip_para_id(NO_3), ip_para_id(eleito))
        self.srv.tratar_eleito(pacote, (NO_3, PORTA_CLUSTER))
        return self.srv.sock_cluster.do_tipo(protocolo.MSG_ELECTED)

    def test_serializacao(self):
        pacote = protocolo.pack_elected(1, 3)
        self.assertEqual(pacote[0], protocolo.MSG_ELECTED)
        self.assertEqual(protocolo.unpack_elected(pacote), (1, 3))

    def test_registra_o_lider_deixa_de_participar_e_repassa_ao_sucessor(self):
        repasses = self.receber_eleito(NO_3)

        self.assertEqual(self.srv.leader_id, ip_para_id(NO_3))
        self.assertFalse(self.srv.em_eleicao)
        self.assertEqual([d[0] for _, d in repasses], [NO_2])
        self.assertEqual(len(self.srv.sock_cluster.do_tipo(protocolo.MSG_ELECTION_ACK)), 1)

    def test_volta_completa_para_no_vencedor(self):
        self.assertEqual(self.receber_eleito(NO_1), [])

    def test_nao_circula_se_o_eleito_caiu(self):
        self.srv.detector.ultimo_heartbeat.pop(ip_para_id(NO_3))
        self.assertEqual(self.receber_eleito(NO_3), [])

    def test_vencedor_anuncia_pelo_sucessor_sem_enviar_coordinator_a_todos(self):
        vencedor = criar_servidor(NO_3, 4403)
        self.addCleanup(vencedor.fechar_sockets)

        vencedor.assumir_lideranca()

        anuncios = vencedor.sock_cluster.do_tipo(protocolo.MSG_ELECTED)
        self.assertEqual([d[0] for _, d in anuncios], [NO_1])
        self.assertEqual(vencedor.sock_cluster.do_tipo(protocolo.MSG_COORDINATOR), [])


class TestEnvioMulticast(unittest.TestCase):
    def criar_fe(self, sock):
        nos = {
            ip_para_id(ip): (ip, PORTA_CLUSTER) for ip in (NO_1, NO_2, NO_3)
        }
        return FrontEnd(NO_1, ip_para_id(NO_1), nos, ip_para_id(NO_1), SocketFalso(), sock)

    def test_primeiro_envio_e_um_unico_datagrama_ao_grupo(self):
        sock = SocketFalso()
        self.criar_fe(sock).multicast_rms(b"ordem")
        self.assertEqual(
            sock.enviados, [(b"ordem", (protocolo.GRUPO_MULTICAST, PORTA_CLUSTER))]
        )

    def test_reenvio_e_unicast_para_cada_rm(self):
        sock = SocketFalso()
        self.criar_fe(sock).multicast_rms(b"ordem", reenvio=True)
        self.assertEqual(sorted(d[0] for _, d in sock.enviados), [NO_2, NO_3])

    def test_erro_no_multicast_cai_para_unicast(self):
        sock = SocketFalso(falhar_no_grupo=True)
        self.criar_fe(sock).multicast_rms(b"ordem")
        self.assertEqual(sorted(d[0] for _, d in sock.enviados), [NO_2, NO_3])

    def test_sem_rms_nada_e_enviado(self):
        sock = SocketFalso()
        fe = self.criar_fe(sock)
        fe.rms_ativos.clear()
        fe.multicast_rms(b"ordem")
        self.assertEqual(sock.enviados, [])


class TestRecepcaoMulticast(unittest.TestCase):
    def test_rm_so_aplica_ordem_vinda_do_lider_atual(self):
        rm = criar_servidor(NO_1, 4404)
        self.addCleanup(rm.fechar_sockets)
        rm.leader_id = ip_para_id(NO_3)
        datagramas = [
            (protocolo.pack_order_multicast(1, 5, 1, 999), (NO_2, PORTA_CLUSTER)),
            (protocolo.pack_order_multicast(1, 7, 1, 10), (NO_3, PORTA_CLUSTER)),
        ]

        class Fila(SocketFalso):
            def recvfrom(self, _):
                if datagramas:
                    return datagramas.pop(0)
                rm.rodando = False
                raise OSError

        rm.sock_cluster_bcast.close()
        rm.sock_cluster_bcast = Fila()
        rm.loop_cluster_bcast()

        buffer = rm.rm.buffer
        self.assertEqual(
            (buffer.expected_seq, buffer.num_reqs, buffer.total_sum), (2, 1, 10)
        )

    def test_datagrama_ao_grupo_chega_a_outro_servidor_na_mesma_maquina(self):
        emissor = servidor.ServidorReplicado(NO_1, 4405, PORTA_CLUSTER + 1)
        self.addCleanup(emissor.fechar_sockets)
        receptor = servidor.ServidorReplicado(NO_2, 4406, PORTA_CLUSTER + 1)
        self.addCleanup(receptor.fechar_sockets)
        receptor.sock_cluster_bcast.settimeout(1)
        pacote = protocolo.pack_order_multicast(1, 5, 1, 10)
        try:
            emissor.sock_cluster.sendto(
                pacote, (protocolo.GRUPO_MULTICAST, PORTA_CLUSTER + 1)
            )
            dados, addr = receptor.sock_cluster_bcast.recvfrom(1024)
        except (OSError, socket.timeout) as erro:
            self.skipTest(f"IP multicast indisponível neste ambiente: {erro!r}")
        self.assertEqual((dados, addr[0]), (pacote, NO_1))


if __name__ == "__main__":
    unittest.main()
