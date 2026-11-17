import threading
import time
import protocolo


class FrontEnd:
    def __init__(self, ip_maq, node_id, nos_cluster, leader_id, sock_cliente, sock_cluster):
        self.ip_maq = ip_maq
        self.node_id = node_id
        self.nos_cluster = nos_cluster
        self.sock_cliente = sock_cliente
        self.sock_cluster = sock_cluster

        # Estruturas de controle do FE
        self.lock_fe = threading.Lock()
        self.next_seq = 1
        self.op_to_seq = {}  # (id_cliente, id_req) -> seq_id
        self.operacoes = {}  # seq_id -> registro de controle e quórum

        # Conjunto de RMs ativos (exceto o FE)
        self.rms_ativos = {no_id for no_id in self.nos_cluster if no_id != leader_id}
        self.ultimo_contato_rm = {no_id: time.monotonic() for no_id in self.rms_ativos}

    def calcular_quorum(self):
        """Por meio da maioria simples dos RMs ativos"""
        m = len(self.rms_ativos)
        if m == 0:
            return 1
        return (m // 2) + 1

    def ativar(self, seq_inicial):
        """Prepara o FE quando este nó vence uma eleição de líder"""
        with self.lock_fe:
            self.next_seq = seq_inicial
            self.op_to_seq.clear()
            self.operacoes.clear()
            self.rms_ativos = {no_id for no_id in self.nos_cluster if no_id != self.node_id}
            agora = time.monotonic()
            self.ultimo_contato_rm = {no_id: agora for no_id in self.rms_ativos}

    def multicast_rms(self, pacote):
        """Envia um datagrama a todos os nós exceto o FE"""
        for rm_id, addr in self.nos_cluster.items():
            if rm_id == self.node_id:
                continue
            try:
                self.sock_cluster.sendto(pacote, addr)
            except OSError:
                pass

    def processar_requisicao_cliente(self, dados, addr, check_lider):
        tipo = dados[0]

        # Descoberta dinâmica do FE
        if tipo == protocolo.MSG_DISCOVERY:
            self.sock_cliente.sendto(protocolo.pack_discovery_resp(), addr)
            return

        if tipo != protocolo.MSG_REQ:
            return

        try:
            id_cliente, id_req, valor = protocolo.unpack_req(dados)
        except Exception:
            return

        chave_op = (id_cliente, id_req)

        with self.lock_fe:
            if chave_op in self.op_to_seq:
                seq_id = self.op_to_seq[chave_op]
                op_info = self.operacoes[seq_id]
                nova_operacao = False
            else:
                seq_id = self.next_seq
                self.next_seq += 1
                self.op_to_seq[chave_op] = seq_id

                op_info = {
                    "rms_que_votaram": set(),
                    "votos_concordantes": {},
                    "resultado_final": None,
                    "evento_quorum": threading.Event(),
                }
                self.operacoes[seq_id] = op_info
                nova_operacao = True

                print(
                    protocolo.colorir(
                        f"[FE {self.ip_maq}] Requisição (client={id_cliente}, req={id_req}, "
                        f"val={valor}) -> seq_id={seq_id} | Quórum: "
                        f"{self.calcular_quorum()}/{len(self.rms_ativos)}",
                        protocolo.COLOR_FE,
                    ),
                    flush=True,
                )

        # Resposta rápida se a operação duplicada já atingiu quórum
        if op_info["evento_quorum"].is_set() and op_info["resultado_final"] is not None:
            num_reqs, total_sum = op_info["resultado_final"]
            self.sock_cliente.sendto(
                protocolo.pack_ack(id_cliente, id_req, num_reqs, total_sum), addr
            )
            return

        # Disseminação via multicast e espera pelo quórum concordante
        pacote_mc = protocolo.pack_order_multicast(seq_id, id_cliente, id_req, valor)
        if nova_operacao:
            self.multicast_rms(pacote_mc)

        tentativas = 0
        while not op_info["evento_quorum"].wait(timeout=0.005):
            if not check_lider():
                return
            tentativas += 1
            self.multicast_rms(pacote_mc)
            if tentativas >= 50:
                return

        if op_info["evento_quorum"].is_set() and op_info["resultado_final"] is not None:
            num_reqs, total_sum = op_info["resultado_final"]
            self.sock_cliente.sendto(
                protocolo.pack_ack(id_cliente, id_req, num_reqs, total_sum), addr
            )

    def tratar_rm_ack(self, dados):
        try:
            seq_id, id_cliente, id_req, num_reqs, total_sum, rm_id = (
                protocolo.unpack_rm_ack(dados)
            )
        except Exception:
            return

        with self.lock_fe:
            if rm_id == self.node_id:
                return

            self.ultimo_contato_rm[rm_id] = time.monotonic()
            if rm_id not in self.rms_ativos:
                self.rms_ativos.add(rm_id)
                ip_rm = self.nos_cluster.get(rm_id, (str(rm_id),))[0]
                print(
                    protocolo.colorir(
                        f"[FE {self.ip_maq}] Ingresso/retorno do RM {ip_rm} | "
                        f"Quórum atual: {self.calcular_quorum()}/{len(self.rms_ativos)}",
                        protocolo.COLOR_FE,
                    ),
                    flush=True,
                )

            op_info = self.operacoes.get(seq_id)
            if op_info is None or op_info["evento_quorum"].is_set():
                return

            if rm_id in op_info["rms_que_votaram"]:
                return
            op_info["rms_que_votaram"].add(rm_id)

            chave_res = (num_reqs, total_sum)
            grupo_votos = op_info["votos_concordantes"].setdefault(chave_res, set())
            grupo_votos.add(rm_id)

            if len(grupo_votos) >= self.calcular_quorum():
                op_info["resultado_final"] = chave_res
                op_info["evento_quorum"].set()

    def verificar_timeouts_rms(self, timeout_rm=2.0):
        agora = time.monotonic()
        with self.lock_fe:
            if not self.operacoes:
                for rm_id in self.rms_ativos:
                    self.ultimo_contato_rm[rm_id] = agora
                return

            falhos = [
                rm_id
                for rm_id in list(self.rms_ativos)
                if (agora - self.ultimo_contato_rm.get(rm_id, agora)) > timeout_rm
            ]
            for rm_id in falhos:
                self.rms_ativos.remove(rm_id)
                ip_rm = self.nos_cluster.get(rm_id, (str(rm_id),))[0]
                print(
                    protocolo.colorir(
                        f"[FE {self.ip_maq}] Falha detectada no RM {ip_rm} | "
                        f"Quórum atual: {self.calcular_quorum()}/{len(self.rms_ativos)}",
                        protocolo.COLOR_FAIL,
                    ),
                    flush=True,
                )