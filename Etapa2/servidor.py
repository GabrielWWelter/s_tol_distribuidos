import os
import sys
import socket
import threading
import time
import struct
import protocolo
import eleicao_anel
from eleicao_anel import ip_para_id
from detector_falhas import DetectorFalhas
from front_end import FrontEnd
from replica_manager import ReplicaManager

INTERVALO_HEARTBEAT = 0.025
TIMEOUT_HEARTBEAT = 0.075
TIMEOUT_ELECTION_ACK = 0.025
TENTATIVAS_ELEICAO = 3
TIMEOUT_ELEICAO = 0.3
TIMEOUT_DESCOBERTA = 0.2


def obter_ip_interface():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def alocar_ip_servidor(port_cluster, ip_preferido=None):
    if ip_preferido is not None:
        candidatos = [ip_preferido]
    else:
        candidatos = [f"127.0.0.{i}" for i in range(1, 255)]
        ip_rede = obter_ip_interface()
        if not ip_rede.startswith("127."):
            candidatos.insert(0, ip_rede)

    for ip in candidatos:
        sock_reserva = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock_reserva.bind((ip, port_cluster))
            return ip, sock_reserva
        except OSError:
            sock_reserva.close()
    raise RuntimeError("Nenhum endereço IP disponível para vincular o servidor.")


class ServidorReplicado:
    def __init__(self, ip_maq, port_cliente, port_cluster, ips_cluster=None, sock_reserva=None):
        self.ip_maq = ip_maq
        self.node_id = ip_para_id(ip_maq)
        self.port_cliente = port_cliente
        self.port_cluster = port_cluster
        self.ips_cluster = ips_cluster or []
        self._sock_reserva = sock_reserva

        self.nos_cluster = {ip_para_id(ip): (ip, port_cluster) for ip in self.ips_cluster}
        self.nos_cluster[self.node_id] = (ip_maq, port_cluster)
        self.leader_id = None
        self.em_eleicao = False
        self.inicio_eleicao = None
        self.detector = DetectorFalhas(TIMEOUT_HEARTBEAT)
        self.ultimo_seq_no = {}
        self.ultimo_sync_enviado = {}
        self.ack_eleicao = threading.Event()
        self.evento_descoberta = threading.Event()
        self.esperando_ack_de = None
        self.lock_eleicao = threading.Lock()
        self.lock_lider = threading.Lock()
        self.lock_nos = threading.Lock()
        self.rodando = True

        # Canal Externo Unicast: ligado ao IP específico da máquina para responder
        # descobertas com o IP correto do FE e receber MSG_REQ diretamente
        self.sock_cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_cliente.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock_cliente.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock_cliente.bind((self.ip_maq, self.port_cliente))

        # Canal Externo Broadcast: ligado em "" (0.0.0.0) para capturar pacotes
        # enviados para 255.255.255.255 ou broadcast da sub-rede
        self.sock_broadcast = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_broadcast.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            self.sock_broadcast.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock_broadcast.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock_broadcast.bind(("", self.port_cliente))

        # Canal Intra-Cluster: FE <-> RMs
        self.sock_cluster = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_cluster.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock_cluster.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock_cluster.bind((self.ip_maq, self.port_cluster))

        # Canal Intra-Cluster Broadcast: descoberta dinâmica de servidores
        self.sock_cluster_bcast = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_cluster_bcast.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            self.sock_cluster_bcast.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock_cluster_bcast.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock_cluster_bcast.bind(("", self.port_cluster))

        self.fe = FrontEnd(
            self.ip_maq,
            self.node_id,
            self.nos_cluster,
            self.node_id,
            self.sock_cliente,
            self.sock_cluster,
        )
        self.rm = ReplicaManager(
            self.ip_maq,
            self.node_id,
            self.nos_cluster,
            self.sock_cluster,
        )

    def fechar_sockets(self):
        self.rodando = False
        for s in (
            self.sock_broadcast,
            self.sock_cliente,
            self.sock_cluster,
            self.sock_cluster_bcast,
            self._sock_reserva,
        ):
            if s is not None:
                try:
                    s.close()
                except OSError:
                    pass

    def is_leader(self):
        return self.node_id == self.leader_id

    def _encaminhar_descoberta_ao_lider(self, addr):
        """Encaminha pedido de descoberta recebido por um RM para que o FE responda."""
        with self.lock_nos:
            fe_addr = self.nos_cluster.get(self.leader_id)
        if fe_addr is None:
            return
        try:
            pct = protocolo.pack_discovery_fwd(addr[0], addr[1])
            self.sock_cluster.sendto(pct, fe_addr)
        except OSError:
            pass

    def loop_broadcast_cliente(self):
        while self.rodando:
            try:
                dados, addr = self.sock_broadcast.recvfrom(1024)
            except OSError:
                if not self.rodando:
                    break
                time.sleep(0.005)
                continue

            if not dados or dados[0] != protocolo.MSG_DISCOVERY:
                continue

            if self.is_leader():
                self.fe.responder_descoberta(addr)
            else:
                self._encaminhar_descoberta_ao_lider(addr)

    def loop_canal_cliente(self):
        while self.rodando:
            try:
                dados, addr = self.sock_cliente.recvfrom(1024)
            except OSError:
                if not self.rodando:
                    break
                time.sleep(0.005)
                continue

            if not dados:
                continue

            if not self.is_leader():
                if dados[0] == protocolo.MSG_DISCOVERY:
                    self._encaminhar_descoberta_ao_lider(addr)
                continue

            threading.Thread(
                target=self.fe.processar_requisicao_cliente,
                args=(dados, addr, self.is_leader),
                daemon=True,
            ).start()

    def loop_cluster_bcast(self):
        while self.rodando:
            try:
                dados, addr = self.sock_cluster_bcast.recvfrom(1024)
            except OSError:
                if not self.rodando:
                    break
                time.sleep(0.005)
                continue

            if not dados:
                continue

            tipo = dados[0]
            if tipo == protocolo.MSG_SERVER_DISCOVERY:
                self.tratar_server_discovery(dados, addr)

    def _enviar_descoberta_cluster(self):
        pct = protocolo.pack_server_discovery(self.node_id)
        destinos = [
            ("<broadcast>", self.port_cluster),
            ("255.255.255.255", self.port_cluster),
            ("127.255.255.255", self.port_cluster),
            ("127.0.0.1", self.port_cluster),
        ]
        for dest in destinos:
            try:
                self.sock_cluster.sendto(pct, dest)
            except OSError:
                pass

    def monitorar_rms(self):
        while self.rodando:
            time.sleep(0.2)
            if not self.rodando:
                break
            if self.is_leader():
                if self.fe.verificar_timeouts_rms():
                    self.propagar_topologia()
                if not self.fe.rms_ativos:
                    self._enviar_descoberta_cluster()
            elif not self.detector.ativos():
                self._enviar_descoberta_cluster()

    def log(self, texto, cor=protocolo.COLOR_ELECTION):
        papel = "FE" if self.is_leader() else "RM"
        print(protocolo.colorir(f"[{papel} {self.ip_maq}] {texto}", cor), flush=True)

    def ip(self, no_id):
        with self.lock_nos:
            return self.nos_cluster.get(no_id, (str(no_id),))[0]

    def enviar_a_todos(self, pacote):
        with self.lock_nos:
            alvos = [
                addr for no_id, addr in self.nos_cluster.items() if no_id != self.node_id
            ]
        for addr in alvos:
            try:
                self.sock_cluster.sendto(pacote, addr)
            except OSError:
                pass

    def inativos(self):
        with self.lock_nos:
            todos = set(self.nos_cluster)
        inativos_set = todos - self.detector.ativos() - {self.node_id}
        if self.em_eleicao and self.leader_id is not None and not self.is_leader():
            inativos_set.add(self.leader_id)
        return inativos_set

    def loop_heartbeat(self):
        while self.rodando:
            with self.lock_lider:
                lider_id = self.leader_id
                ultimo_seq = (
                    self.fe.obter_ultimo_seq_confirmado()
                    if lider_id == self.node_id
                    else self.rm.obter_proximo_seq() - 1
                )
            self.enviar_a_todos(protocolo.pack_heartbeat(lider_id or 0, ultimo_seq))
            if not self.rodando:
                break
            for no_id in self.detector.verificar():
                if not self.rodando:
                    break
                if self.is_leader():
                    if self.fe.marcar_rm(no_id, False):
                        self.propagar_topologia()
                elif no_id == self.leader_id and not self.em_eleicao:
                    self.em_eleicao = True
                    self.inicio_eleicao = time.monotonic()
                    self.log(f"Detecção de falha no front-end {self.ip(no_id)}", protocolo.COLOR_FAIL)
                    self.log("Início da execução do algoritmo de eleição", protocolo.COLOR_ELECTION)
                    threading.Thread(target=self.propagar_eleicao, args=(self.node_id,), daemon=True).start()
            if not self.rodando:
                break
            if (
                self.em_eleicao
                and not self.is_leader()
                and self.inicio_eleicao is not None
                and (time.monotonic() - self.inicio_eleicao) > TIMEOUT_ELEICAO
            ):
                self.inicio_eleicao = time.monotonic()
                self.log("Timeout na eleição em andamento; reiniciando eleição no anel", protocolo.COLOR_FAIL)
                threading.Thread(target=self.propagar_eleicao, args=(self.node_id,), daemon=True).start()
            time.sleep(INTERVALO_HEARTBEAT)

    def propagar_topologia(self):
        ips = sorted(self.fe.obter_ips_cluster(), key=ip_para_id)
        ativos_ids = {ip_para_id(ip_str) for ip_str in ips}
        destinos = []
        with self.lock_nos:
            for no_id in list(self.nos_cluster):
                if no_id not in ativos_ids and no_id != self.node_id:
                    self.nos_cluster.pop(no_id, None)
            for no_id in ativos_ids:
                if no_id != self.node_id and no_id in self.nos_cluster:
                    destinos.append(self.nos_cluster[no_id])
        pct = protocolo.pack_topology(self.node_id, ips)
        for addr_rm in destinos:
            try:
                self.sock_cluster.sendto(pct, addr_rm)
            except OSError:
                pass

    def _enviar_join_ao_lider(self, lider_id):
        with self.lock_nos:
            fe_addr = self.nos_cluster.get(lider_id)
        if fe_addr is None:
            return
        pct_join = protocolo.pack_rm_join(self.node_id, self.port_cluster)
        try:
            self.sock_cluster.sendto(pct_join, fe_addr)
        except OSError:
            pass

    def _anunciar_lideranca_para(self, addr):
        ultimo_seq = self.fe.obter_ultimo_seq_confirmado()
        try:
            self.sock_cluster.sendto(
                protocolo.pack_heartbeat(self.node_id, ultimo_seq), addr
            )
            self.sock_cluster.sendto(
                protocolo.pack_coordinator(self.node_id), addr
            )
        except OSError:
            pass

    def _reintegrar_rm(self, rm_id, rm_ip, rm_port, forcar_sync=False):
        agora = time.monotonic()
        if not forcar_sync and (agora - self.ultimo_sync_enviado.get(rm_id, 0.0)) < 0.2:
            return
        self.ultimo_sync_enviado[rm_id] = agora

        with self.lock_nos:
            self.nos_cluster[rm_id] = (rm_ip, rm_port)
        self.detector.registrar(rm_id)

        alterou, (sync_seq, num_reqs, total_sum), pendentes = self.fe.reintegrar_rm(
            rm_id, rm_ip, rm_port
        )
        pct_sync = protocolo.pack_state_sync(sync_seq, num_reqs, total_sum)
        try:
            self.sock_cluster.sendto(pct_sync, (rm_ip, rm_port))
        except OSError:
            pass

        for pct_op in pendentes:
            try:
                self.sock_cluster.sendto(pct_op, (rm_ip, rm_port))
            except OSError:
                pass

        if alterou or forcar_sync:
            self.propagar_topologia()

    def tratar_topologia(self, dados, addr=None):
        leader_id, ips = protocolo.unpack_topology(dados)
        if leader_id == self.node_id:
            return
        with self.lock_nos:
            if leader_id not in self.nos_cluster:
                ip_lider = addr[0] if addr is not None else eleicao_anel.id_para_ip(leader_id)
                self.nos_cluster[leader_id] = (ip_lider, self.port_cluster)
        if self.leader_id is not None and leader_id != self.leader_id:
            via_coord = not self.is_leader() and (
                self.leader_id not in self.detector.ativos() or self.em_eleicao
            )
            if not self.definir_lider(leader_id, via_coordenador=via_coord):
                if self.is_leader() and addr is not None:
                    self._anunciar_lideranca_para((addr[0], self.port_cluster))
                return
        else:
            with self.lock_lider:
                self.leader_id = leader_id
                self.em_eleicao = False
                self.inicio_eleicao = None
        novos_nos = {self.node_id: (self.ip_maq, self.port_cluster)}
        for ip_str in ips:
            nid = ip_para_id(ip_str)
            novos_nos[nid] = (ip_str, self.port_cluster)
        with self.lock_nos:
            for no_id in list(self.nos_cluster):
                if no_id not in novos_nos:
                    del self.nos_cluster[no_id]
            self.nos_cluster.update(novos_nos)
        self.log(f"Topologia do anel atualizada: {ips}", protocolo.COLOR_RM)

    def tratar_server_discovery(self, dados, addr):
        sender_id = protocolo.unpack_server_discovery(dados)
        if sender_id == self.node_id:
            return
        if self.is_leader():
            pct = protocolo.pack_server_discovery_resp(self.node_id)
            try:
                self.sock_cluster.sendto(pct, addr)
            except OSError:
                pass
            self._anunciar_lideranca_para((addr[0], self.port_cluster))

    def tratar_server_discovery_resp(self, dados, addr):
        if self.leader_id is not None:
            return
        lider_id = protocolo.unpack_server_discovery_resp(dados)
        with self.lock_nos:
            self.nos_cluster[lider_id] = (addr[0], self.port_cluster)
        self.leader_id = lider_id
        self.detector.registrar(lider_id)
        self.evento_descoberta.set()

    def tratar_rm_join(self, dados, addr):
        if not self.is_leader():
            return
        rm_id, rm_port = protocolo.unpack_rm_join(dados)
        self._reintegrar_rm(rm_id, addr[0], rm_port, forcar_sync=True)

    def descobrir_cluster_inicial(self):
        self._enviar_descoberta_cluster()

        if not self.evento_descoberta.wait(TIMEOUT_DESCOBERTA):
            self.assumir_lideranca_inicial()
        else:
            self.log(
                f"Front-End ativo encontrado em {self.ip(self.leader_id)}. Ingressando como Replica Manager.",
                protocolo.COLOR_RM,
            )
            self._enviar_join_ao_lider(self.leader_id)

    def assumir_lideranca_inicial(self):
        with self.lock_lider:
            self.leader_id = self.node_id
            self.em_eleicao = False
            self.fe.ativar(seq_inicial=1, historico={}, rms_ativos=set())
            self.log(
                "Nenhum servidor ativo encontrado. Autoproclamado Front-End inicial (0 RMs).",
                protocolo.COLOR_LEADER,
            )

    def propagar_eleicao(self, candidato, eleito=None):
        """Envia <election, candidato> ou, se eleito for dado, <elected, eleito>."""
        if eleito is None:
            self.em_eleicao = True  # participante
            if self.inicio_eleicao is None:
                self.inicio_eleicao = time.monotonic()
            pacote = protocolo.pack_election(self.node_id, candidato)
        else:
            pacote = protocolo.pack_elected(self.node_id, eleito)
        with self.lock_eleicao:
            inativos = self.inativos()
            try:
                while True:
                    with self.lock_nos:
                        nos_ids = list(self.nos_cluster)
                        sucessor = eleicao_anel.sucessor(nos_ids, self.node_id, inativos)
                        addr_sucessor = self.nos_cluster.get(sucessor) if sucessor is not None else None
                    if sucessor is None:
                        break
                    if eleito is None:
                        self.log(f"Enviando eleição a {self.ip(sucessor)} (candidato={self.ip(candidato)})")
                    else:
                        self.log(f"Enviando eleito a {self.ip(sucessor)} (eleito={self.ip(eleito)})")
                    self.esperando_ack_de = sucessor
                    for _ in range(TENTATIVAS_ELEICAO):
                        self.ack_eleicao.clear()
                        if addr_sucessor is not None:
                            try:
                                self.sock_cluster.sendto(pacote, addr_sucessor)
                            except OSError:
                                pass
                        if self.ack_eleicao.wait(TIMEOUT_ELECTION_ACK):
                            return
                    self.log(f"Sucessor {self.ip(sucessor)} não confirmou; pulando no anel", protocolo.COLOR_FAIL)
                    inativos.add(sucessor)
            finally:
                self.esperando_ack_de = None
        if eleito is None:
            self.assumir_lideranca()

    def tratar_eleicao(self, dados, addr):
        try:
            remetente, candidato = protocolo.unpack_election(dados)
        except struct.error:
            return
        with self.lock_nos:
            conhecidos = remetente in self.nos_cluster and candidato in self.nos_cluster
        if not conhecidos:
            return
        self.detector.registrar(remetente)
        try:
            self.sock_cluster.sendto(protocolo.pack_election_ack(self.node_id), addr)
        except OSError:
            pass
        if self.is_leader():
            try:
                self.sock_cluster.sendto(protocolo.pack_coordinator(self.node_id), addr)
            except OSError:
                pass
            return
        participante = self.em_eleicao
        self.em_eleicao = True
        if self.inicio_eleicao is None:
            self.inicio_eleicao = time.monotonic()
        self.log(f"Eleição recebida de {self.ip(remetente)} (candidato={self.ip(candidato)})")
        acao, candidato = eleicao_anel.tratar_eleicao(
            self.node_id, candidato, self.inativos(), participante
        )
        if acao == "descartar":
            self.log("Eleição descartada: já sou participante e meu id é maior que o candidato")
        elif acao == "eleito":
            self.assumir_lideranca()
        else:
            self.propagar_eleicao(candidato)

    def tratar_eleito(self, dados, addr):
        try:
            remetente, eleito = protocolo.unpack_elected(dados)
        except struct.error:
            return
        with self.lock_nos:
            conhecidos = remetente in self.nos_cluster and eleito in self.nos_cluster
        if not conhecidos:
            return
        self.detector.registrar(remetente)
        try:
            self.sock_cluster.sendto(protocolo.pack_election_ack(self.node_id), addr)
        except OSError:
            pass
        if eleito == self.node_id:
            self.log("Mensagem de eleito completou a volta no anel")
            return
        if eleito not in self.detector.ativos():
            return  # o eleito caiu: a mensagem não pode circular para sempre
        self.log(f"Eleito recebido de {self.ip(remetente)} (eleito={self.ip(eleito)})")
        if self.definir_lider(eleito, via_coordenador=True):
            self.propagar_eleicao(None, eleito=eleito)

    def assumir_lideranca(self):
        with self.lock_lider:
            if not self.is_leader():
                with self.rm.lock_rm:
                    buffer = self.rm.buffer
                    buffer.limpar_pendencias_nao_aplicadas()
                    maior_seq = buffer.expected_seq
                    for no_id in self.detector.ativos() - {self.node_id, self.leader_id}:
                        seq_no = self.ultimo_seq_no.get(no_id, -1) + 1
                        if seq_no > maior_seq:
                            maior_seq = seq_no

                    rms_em_dia = {
                        no_id
                        for no_id in self.detector.ativos() - {self.node_id, self.leader_id}
                        if self.ultimo_seq_no.get(no_id, -1) >= maior_seq - 1
                    }
                    self.fe.ativar(
                        maior_seq,
                        buffer.historico,
                        rms_em_dia,
                        buffer.num_reqs,
                        buffer.total_sum,
                    )
                self.leader_id = self.node_id
                self.em_eleicao = False
                self.inicio_eleicao = None
                self.log(
                    f"Proclamação do novo coordenador: {self.ip_maq} | "
                    f"Assumindo Front-End a partir do seq_id={self.fe.next_seq} | "
                    f"Quórum atual: {self.fe.formatar_quorum()}",
                    protocolo.COLOR_LEADER,
                )
        self.propagar_topologia()
        # <elected> dá uma volta no anel, como nos slides
        threading.Thread(
            target=self.propagar_eleicao, args=(None, self.node_id), daemon=True
        ).start()

    def definir_lider(self, lider_id, via_coordenador=False):
        with self.lock_nos:
            conhecido = lider_id in self.nos_cluster
        if not conhecido:
            return False
        rebaixou_lider = False
        with self.lock_lider:
            seq_remoto = self.ultimo_seq_no.get(lider_id, -1)
            if self.is_leader():
                if lider_id == self.node_id:
                    return True
                seq_local = self.fe.obter_ultimo_seq_confirmado()
                if seq_remoto < seq_local or (lider_id < self.node_id and seq_remoto <= seq_local):
                    return False
                rebaixou_lider = True
            else:
                seq_rm = self.rm.obter_proximo_seq() - 1
                if lider_id in self.ultimo_seq_no and seq_remoto < seq_rm:
                    return False
                if (
                    not via_coordenador
                    and self.leader_id is not None
                    and self.leader_id in self.detector.ativos()
                    and not self.em_eleicao
                ):
                    if lider_id < self.leader_id or seq_remoto < seq_rm:
                        return False
            mudou = lider_id != self.leader_id
            self.leader_id = lider_id
            self.em_eleicao = False
            self.inicio_eleicao = None
            if mudou:
                self.log(f"Novo líder: FE={self.ip(lider_id)}", protocolo.COLOR_LEADER)
        if rebaixou_lider:
            sync_seq, sync_nr, sync_ts = self.fe.obter_estado_sincronizacao()
            self.fe.desativar()
            with self.rm.lock_rm:
                if sync_seq > self.rm.buffer.expected_seq:
                    self.rm.buffer.sincronizar_estado(sync_seq, sync_nr, sync_ts)
            self.enviar_a_todos(protocolo.pack_coordinator(lider_id))
            self._enviar_join_ao_lider(lider_id)
        return True

    def tratar_heartbeat(self, dados, addr):
        lider_remoto, ultimo_seq = protocolo.unpack_heartbeat(dados)
        no_id = ip_para_id(addr[0])
        if no_id == self.node_id:
            return
        with self.lock_nos:
            if no_id not in self.nos_cluster:
                self.nos_cluster[no_id] = (addr[0], self.port_cluster)
        self.detector.registrar(no_id)
        self.ultimo_seq_no[no_id] = ultimo_seq

        if lider_remoto == no_id:
            if not self.definir_lider(no_id, via_coordenador=False):
                if self.is_leader():
                    self._anunciar_lideranca_para((addr[0], self.port_cluster))
            return

        if not self.is_leader():
            if no_id == self.leader_id and lider_remoto != 0 and lider_remoto != no_id:
                ip_novo_lider = eleicao_anel.id_para_ip(lider_remoto)
                with self.lock_nos:
                    self.nos_cluster[lider_remoto] = (ip_novo_lider, self.port_cluster)
                if self.definir_lider(lider_remoto, via_coordenador=True):
                    self._enviar_join_ao_lider(lider_remoto)
            return

        if lider_remoto != self.node_id:
            self._anunciar_lideranca_para((addr[0], self.port_cluster))
            return

        if no_id not in self.fe.rms_ativos:
            proximo_seq = self.fe.obter_proximo_seq()
            if ultimo_seq >= proximo_seq - 1:
                if self.fe.marcar_rm(no_id, True):
                    self.propagar_topologia()
            else:
                self._reintegrar_rm(no_id, addr[0], self.port_cluster, forcar_sync=False)

    def tratar_controle(self, tipo, dados, addr):
        try:
            if tipo == protocolo.MSG_HEARTBEAT:
                self.tratar_heartbeat(dados, addr)
            elif tipo == protocolo.MSG_SERVER_DISCOVERY:
                self.tratar_server_discovery(dados, addr)
            elif tipo == protocolo.MSG_SERVER_DISCOVERY_RESP:
                self.tratar_server_discovery_resp(dados, addr)
            elif tipo == protocolo.MSG_RM_JOIN:
                self.tratar_rm_join(dados, addr)
            elif tipo == protocolo.MSG_TOPOLOGY:
                self.tratar_topologia(dados, addr)
            elif tipo == protocolo.MSG_STATE_SYNC and not self.is_leader():
                next_seq, num_reqs, total_sum = protocolo.unpack_state_sync(dados)
                self.rm.sincronizar(next_seq, num_reqs, total_sum, self.leader_id)
            elif tipo == protocolo.MSG_ELECTION:
                threading.Thread(target=self.tratar_eleicao, args=(dados, addr), daemon=True).start()
            elif tipo == protocolo.MSG_ELECTED:
                threading.Thread(target=self.tratar_eleito, args=(dados, addr), daemon=True).start()
            elif tipo == protocolo.MSG_ELECTION_ACK:
                if protocolo.unpack_election_ack(dados) == self.esperando_ack_de:
                    self.ack_eleicao.set()
            elif tipo == protocolo.MSG_COORDINATOR:
                lider_id = protocolo.unpack_coordinator(dados)
                with self.lock_nos:
                    if lider_id not in self.nos_cluster:
                        self.nos_cluster[lider_id] = (addr[0], self.port_cluster)
                if self.definir_lider(lider_id, via_coordenador=True):
                    if not self.is_leader():
                        pct_hb = protocolo.pack_heartbeat(
                            self.leader_id, self.rm.obter_proximo_seq() - 1
                        )
                        try:
                            self.sock_cluster.sendto(pct_hb, addr)
                        except OSError:
                            pass
                elif self.is_leader():
                    self._anunciar_lideranca_para((addr[0], self.port_cluster))
        except (struct.error, KeyError, OSError):
            pass

    def iniciar(self):
        threading.Thread(target=self.loop_broadcast_cliente, daemon=True).start()
        threading.Thread(target=self.loop_canal_cliente, daemon=True).start()
        threading.Thread(target=self.loop_cluster_bcast, daemon=True).start()
        threading.Thread(target=self.monitorar_rms, daemon=True).start()

        # Thread de recepção do cluster unicast
        def loop_recv_cluster():
            while self.rodando:
                try:
                    dados, addr = self.sock_cluster.recvfrom(1024)
                except OSError:
                    if not self.rodando:
                        break
                    time.sleep(0.005)
                    continue

                if not dados:
                    continue

                tipo = dados[0]
                if tipo == protocolo.MSG_ORDER_MULTICAST and not self.is_leader():
                    self.rm.tratar_order_multicast(dados, addr, self.leader_id)
                elif tipo == protocolo.MSG_STATE_SYNC and not self.is_leader():
                    next_seq, num_reqs, total_sum = protocolo.unpack_state_sync(dados)
                    self.rm.sincronizar(next_seq, num_reqs, total_sum, self.leader_id)
                elif tipo == protocolo.MSG_RM_ACK and self.is_leader():
                    self.fe.tratar_rm_ack(dados)
                elif tipo == protocolo.MSG_NACK and self.is_leader():
                    self.fe.tratar_nack(dados, addr)
                elif tipo == protocolo.MSG_DISCOVERY_FWD and self.is_leader():
                    try:
                        client_addr = protocolo.unpack_discovery_fwd(dados)
                        self.fe.responder_descoberta(client_addr)
                    except Exception:
                        pass
                else:
                    self.tratar_controle(tipo, dados, addr)

        threading.Thread(target=loop_recv_cluster, daemon=True).start()

        # Descoberta inicial antes de disparar os heartbeats
        self.descobrir_cluster_inicial()
        threading.Thread(target=self.loop_heartbeat, daemon=True).start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.fechar_sockets()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Uso: python3 {sys.argv[0]} <port_cliente> [port_cluster]")
        sys.exit(1)

    port_cliente = int(sys.argv[1])
    port_cluster = int(sys.argv[2]) if len(sys.argv) > 2 else port_cliente + 1000
    ip_maq, sock_reserva = alocar_ip_servidor(port_cluster)

    servidor = ServidorReplicado(
        ip_maq, port_cliente, port_cluster, sock_reserva=sock_reserva
    )
    try:
        servidor.iniciar()
    except KeyboardInterrupt:
        pass
    finally:
        servidor.fechar_sockets()
        os._exit(0)