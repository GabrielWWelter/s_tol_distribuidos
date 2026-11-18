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

INTERVALO_HEARTBEAT = 0.1
TIMEOUT_HEARTBEAT = 0.5
TIMEOUT_ELECTION_ACK = 0.1
TENTATIVAS_ELEICAO = 3


class ServidorReplicado:
    def __init__(self, ip_maq, port_cliente, port_cluster, ips_cluster):
        self.ip_maq = ip_maq
        self.node_id = ip_para_id(ip_maq)
        self.port_cliente = port_cliente
        self.port_cluster = port_cluster
        self.ips_cluster = ips_cluster

        self.nos_cluster = {ip_para_id(ip): (ip, port_cluster) for ip in ips_cluster}
        self.nos_cluster[self.node_id] = (ip_maq, port_cluster)
        self.leader_id = None
        self.detector = DetectorFalhas(TIMEOUT_HEARTBEAT)
        self.ultimo_seq_no = {}
        self.ack_eleicao = threading.Event()
        self.esperando_ack_de = None
        self.lock_eleicao = threading.Lock()
        self.lock_lider = threading.Lock()

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
        self.sock_cluster.bind((self.ip_maq, self.port_cluster))

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

    def is_leader(self):
        return self.node_id == self.leader_id

    def _encaminhar_descoberta_ao_lider(self, addr):
        """Encaminha pedido de descoberta recebido por um RM para que o FE responda."""
        fe_addr = self.nos_cluster.get(self.leader_id)
        if fe_addr is None:
            return
        try:
            pct = protocolo.pack_discovery_fwd(addr[0], addr[1])
            self.sock_cluster.sendto(pct, fe_addr)
        except OSError:
            pass

    def loop_broadcast_cliente(self):
        """Escuta pacotes de descobrimento por broadcast na porta do cliente."""
        while True:
            try:
                dados, addr = self.sock_broadcast.recvfrom(1024)
            except OSError:
                continue

            if not dados or dados[0] != protocolo.MSG_DISCOVERY:
                continue

            if self.is_leader():
                self.fe.responder_descoberta(addr)
            else:
                self._encaminhar_descoberta_ao_lider(addr)

    def loop_canal_cliente(self):
        """Escuta o canal unicast externo e cria uma thread por requisição recebida."""
        while True:
            try:
                dados, addr = self.sock_cliente.recvfrom(1024)
            except OSError:
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

    def monitorar_rms(self):
        """Monitora periodicamente a vitalidade dos RMs quando atua como FE."""
        while True:
            time.sleep(0.5)
            if self.is_leader():
                self.fe.verificar_timeouts_rms()

    def log(self, texto, cor=protocolo.COLOR_ELECTION):
        papel = "FE" if self.is_leader() else "RM"
        print(protocolo.colorir(f"[{papel} {self.ip_maq}] {texto}", cor), flush=True)

    def ip(self, no_id):
        return self.nos_cluster[no_id][0]

    def enviar_a_todos(self, pacote):
        for no_id, addr in self.nos_cluster.items():
            if no_id != self.node_id:
                try:
                    self.sock_cluster.sendto(pacote, addr)
                except OSError:
                    pass

    def inativos(self):
        return set(self.nos_cluster) - self.detector.ativos() - {self.node_id}

    def loop_heartbeat(self):
        while True:
            with self.lock_lider:
                lider_id = self.leader_id
                ultimo_seq = (
                    self.fe.obter_proximo_seq()
                    if lider_id == self.node_id
                    else self.rm.obter_proximo_seq()
                ) - 1
            self.enviar_a_todos(protocolo.pack_heartbeat(lider_id or 0, ultimo_seq))
            for no_id in self.detector.verificar():
                if self.is_leader():
                    self.fe.marcar_rm(no_id, False)
                elif no_id == self.leader_id:
                    self.log(f"Falha detectada no FE {self.ip(no_id)}; iniciando eleição", protocolo.COLOR_FAIL)
                    threading.Thread(target=self.propagar_eleicao, args=(self.node_id,), daemon=True).start()
            time.sleep(INTERVALO_HEARTBEAT)

    def eleger_lider_inicial(self):
        time.sleep(TIMEOUT_HEARTBEAT)
        if self.leader_id is None:
            self.log("Nenhum FE ativo encontrado; iniciando eleição")
            self.propagar_eleicao(self.node_id)

    def propagar_eleicao(self, candidato):
        pacote = protocolo.pack_election(self.node_id, candidato)
        with self.lock_eleicao:
            inativos = self.inativos()
            try:
                while (sucessor := eleicao_anel.sucessor(self.nos_cluster, self.node_id, inativos)) is not None:
                    self.log(f"Enviando eleição a {self.ip(sucessor)} (candidato={self.ip(candidato)})")
                    self.esperando_ack_de = sucessor
                    for _ in range(TENTATIVAS_ELEICAO):
                        self.ack_eleicao.clear()
                        self.sock_cluster.sendto(pacote, self.nos_cluster[sucessor])
                        if self.ack_eleicao.wait(TIMEOUT_ELECTION_ACK):
                            return
                    self.log(f"Sucessor {self.ip(sucessor)} não confirmou; pulando no anel", protocolo.COLOR_FAIL)
                    inativos.add(sucessor)
            finally:
                self.esperando_ack_de = None
        self.assumir_lideranca()

    def tratar_eleicao(self, dados, addr):
        try:
            remetente, candidato = protocolo.unpack_election(dados)
        except struct.error:
            return
        if remetente not in self.nos_cluster or candidato not in self.nos_cluster:
            return
        self.detector.registrar(remetente)
        self.sock_cluster.sendto(protocolo.pack_election_ack(self.node_id), addr)
        self.log(f"Eleição recebida de {self.ip(remetente)} (candidato={self.ip(candidato)})")
        acao, candidato = eleicao_anel.tratar_eleicao(self.node_id, candidato, self.inativos())
        if acao == "eleito":
            self.assumir_lideranca()
        else:
            self.propagar_eleicao(candidato)

    def assumir_lideranca(self):
        with self.lock_lider:
            if not self.is_leader():
                with self.rm.lock_rm:
                    buffer = self.rm.buffer
                    rms_em_dia = {
                        no_id
                        for no_id in self.detector.ativos() - {self.node_id, self.leader_id}
                        if self.ultimo_seq_no.get(no_id, -1) >= buffer.expected_seq - 1
                    }
                    self.fe.ativar(buffer.expected_seq, buffer.historico, rms_em_dia)
                self.leader_id = self.node_id
                self.log(
                    f"Eleito novo líder; assumindo o Front-End a partir do seq_id={self.fe.next_seq} | "
                    f"Quórum atual: {self.fe.formatar_quorum()}",
                    protocolo.COLOR_LEADER,
                )
        self.enviar_a_todos(protocolo.pack_coordinator(self.node_id))

    def definir_lider(self, lider_id):
        if lider_id not in self.nos_cluster:
            return
        with self.lock_lider:
            seq_remoto = self.ultimo_seq_no.get(lider_id, -1)
            if self.is_leader():
                seq_local = self.fe.obter_proximo_seq() - 1
                if seq_remoto < seq_local or (lider_id < self.node_id and seq_remoto <= seq_local):
                    return
            elif self.leader_id is not None and self.leader_id in self.detector.ativos():
                if lider_id < self.leader_id or seq_remoto < self.rm.obter_proximo_seq() - 1:
                    return
            if lider_id != self.leader_id:
                self.leader_id = lider_id
                self.log(f"Novo líder: FE={self.ip(lider_id)}", protocolo.COLOR_LEADER)

    def tratar_heartbeat(self, dados, addr):
        lider_remoto, ultimo_seq = protocolo.unpack_heartbeat(dados)
        no_id = ip_para_id(addr[0])
        if no_id not in self.nos_cluster:
            return
        self.detector.registrar(no_id)
        self.ultimo_seq_no[no_id] = ultimo_seq
        if self.is_leader() and ultimo_seq >= self.fe.obter_proximo_seq() - 1:
            self.fe.marcar_rm(no_id, True)
        if lider_remoto == no_id:
            self.definir_lider(no_id)

    def tratar_controle(self, tipo, dados, addr):
        try:
            if tipo == protocolo.MSG_HEARTBEAT:
                self.tratar_heartbeat(dados, addr)
            elif tipo == protocolo.MSG_ELECTION:
                threading.Thread(target=self.tratar_eleicao, args=(dados, addr), daemon=True).start()
            elif tipo == protocolo.MSG_ELECTION_ACK:
                if protocolo.unpack_election_ack(dados) == self.esperando_ack_de:
                    self.ack_eleicao.set()
            elif tipo == protocolo.MSG_COORDINATOR:
                self.definir_lider(protocolo.unpack_coordinator(dados))
        except (struct.error, KeyError, OSError):
            pass

    def iniciar(self):
        threading.Thread(target=self.loop_broadcast_cliente, daemon=True).start()
        threading.Thread(target=self.loop_canal_cliente, daemon=True).start()
        threading.Thread(target=self.monitorar_rms, daemon=True).start()
        threading.Thread(target=self.loop_heartbeat, daemon=True).start()
        threading.Thread(target=self.eleger_lider_inicial, daemon=True).start()
        self.log("Iniciado; aguardando heartbeats do cluster", protocolo.COLOR_RM)

        try:
            while True:
                try:
                    dados, addr = self.sock_cluster.recvfrom(1024)
                except OSError:
                    continue

                if not dados:
                    continue

                tipo = dados[0]
                if tipo == protocolo.MSG_ORDER_MULTICAST and not self.is_leader():
                    self.rm.tratar_order_multicast(dados, addr, self.leader_id)
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
        except KeyboardInterrupt:
            pass
        finally:
            self.sock_broadcast.close()
            self.sock_cliente.close()
            self.sock_cluster.close()


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(
            f"Uso: python3 {sys.argv[0]} <ip_maq> <port_cliente> <port_cluster> <ip1> <ip2> [<ip3> ...]"
        )
        sys.exit(1)

    servidor = ServidorReplicado(
        sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4:]
    )
    servidor.iniciar()