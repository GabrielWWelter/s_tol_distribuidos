import threading
import time
import protocolo


class FrontEnd:
    def __init__(
        self,
        ip_maq,
        node_id,
        nos_cluster,
        leader_id,
        sock_cliente,
        sock_cluster,
        max_historico=10000,
    ):
        self.ip_maq = ip_maq
        self.node_id = node_id
        self.nos_cluster = nos_cluster
        self.sock_cliente = sock_cliente
        self.sock_cluster = sock_cluster
        self.max_historico = max_historico

        self.lock_fe = threading.Lock()
        self.next_seq = 1
        self.num_reqs = 0
        self.total_sum = 0
        self.op_to_seq = {}  # (id_cliente, id_req) -> seq_id
        self.operacoes = {}  # seq_id -> registro de controle e quórum
        self.pendentes = set()  # seq_id ainda sem quórum atingido
        # FE eleito atrasado: totais locais não confiáveis até este seq ser confirmado
        self.seq_sem_estado = None

        # O nó atuando como FE não integra o conjunto de RMs ativos
        self.rms_ativos = {no_id for no_id in self.nos_cluster if no_id != leader_id}
        self.ultimo_contato_rm = {no_id: time.monotonic() for no_id in self.rms_ativos}
        self.aguardando_desde = {no_id: None for no_id in self.rms_ativos}

    def calcular_quorum(self):
        """Maioria simples dos RMs ativos."""
        m = len(self.rms_ativos)
        if m == 0:
            return None
        return (m // 2) + 1

    def formatar_quorum(self):
        q = self.calcular_quorum()
        m = len(self.rms_ativos)
        return f"{q if q is not None else '-'}/{m}"

    def obter_proximo_seq(self):
        with self.lock_fe:
            return self.next_seq

    def ativar(self, seq_inicial, historico, rms_ativos, num_reqs_base=0, total_sum_base=0, atrasado=False):
        """Prepara o FE quando este nó vence uma eleição de líder."""
        with self.lock_fe:
            self.seq_sem_estado = seq_inicial if atrasado else None
            self.next_seq = seq_inicial
            self.op_to_seq.clear()
            self.operacoes.clear()
            self.pendentes.clear()
            max_nr, max_ts = num_reqs_base, total_sum_base
            for seq_id, (dados_op, (_, _, _, num_reqs, total_sum)) in historico.items():
                evento = threading.Event()
                evento.set()
                self.op_to_seq[dados_op[:2]] = seq_id
                self.operacoes[seq_id] = {
                    "dados_op": dados_op,
                    "rms_que_votaram": set(),
                    "votos_concordantes": {},
                    "resultado_final": (num_reqs, total_sum),
                    "evento_quorum": evento,
                    "em_andamento": False,
                }
                if num_reqs > max_nr:
                    max_nr, max_ts = num_reqs, total_sum
            self.num_reqs = max_nr
            self.total_sum = max_ts
            self.rms_ativos = set(rms_ativos)
            agora = time.monotonic()
            self.ultimo_contato_rm = {no_id: agora for no_id in self.rms_ativos}
            self.aguardando_desde = {no_id: None for no_id in self.rms_ativos}

    def desativar(self):
        with self.lock_fe:
            self.rms_ativos.clear()
            self.ultimo_contato_rm.clear()
            self.aguardando_desde.clear()
            self.pendentes.clear()
            for op_info in self.operacoes.values():
                op_info["evento_quorum"].set()

    def responder_descoberta(self, addr):
        """Responde ao cliente pelo socket unicast ligado ao IP do FE."""
        try:
            self.sock_cliente.sendto(protocolo.pack_discovery_resp(), addr)
            print(
                protocolo.colorir(
                    f"[FE {self.ip_maq}] Descoberta por broadcast respondida para {addr[0]}:{addr[1]}",
                    protocolo.COLOR_FE,
                ),
                flush=True,
            )
        except OSError:
            pass

    def multicast_rms(self, pacote, reenvio=False):
        """Dissemina a operação via IP multicast; os reenvios vão por unicast a
        cada RM ativo, o que cobre redes que bloqueiam multicast."""
        agora = time.monotonic()
        with self.lock_fe:
            destinos = []
            for rm_id in self.rms_ativos:
                addr_rm = self.nos_cluster.get(rm_id)
                if addr_rm is not None:
                    destinos.append(addr_rm)
                    if self.aguardando_desde.get(rm_id) is None:
                        self.aguardando_desde[rm_id] = agora

        if destinos and not reenvio:
            try:
                self.sock_cluster.sendto(pacote, (protocolo.GRUPO_MULTICAST, destinos[0][1]))
                return
            except OSError:
                pass

        for addr in destinos:
            try:
                self.sock_cluster.sendto(pacote, addr)
            except OSError:
                pass

    def processar_requisicao_cliente(self, dados, addr, check_lider):
        try:
            tipo = dados[0]

            if tipo == protocolo.MSG_DISCOVERY:
                self.responder_descoberta(addr)
                return

            if tipo != protocolo.MSG_REQ:
                return

            try:
                id_cliente, id_req, valor = protocolo.unpack_req(dados)
            except Exception:
                return

            chave_op = (id_cliente, id_req)
            conduzir_multicast = False
            log_recebida = None

            with self.lock_fe:
                if chave_op in self.op_to_seq:
                    seq_id = self.op_to_seq[chave_op]
                    op_info = self.operacoes[seq_id]
                    if not op_info["evento_quorum"].is_set() and not op_info["em_andamento"]:
                        op_info["em_andamento"] = True
                        conduzir_multicast = True
                else:
                    seq_id = self.next_seq
                    self.next_seq += 1
                    self.op_to_seq[chave_op] = seq_id

                    op_info = {
                        "dados_op": (id_cliente, id_req, valor),
                        "rms_que_votaram": set(),
                        "votos_concordantes": {},
                        "resultado_final": None,
                        "evento_quorum": threading.Event(),
                        "em_andamento": True,
                    }
                    self.operacoes[seq_id] = op_info
                    self.pendentes.add(seq_id)
                    conduzir_multicast = True
                    log_recebida = (
                        f"[FE {self.ip_maq}] Requisição recebida (client={id_cliente}, "
                        f"req={id_req}, val={valor}) -> seq_id={seq_id} | "
                        f"Quórum atual: {self.formatar_quorum()}"
                    )

            if log_recebida is not None:
                print(protocolo.colorir(log_recebida, protocolo.COLOR_FE), flush=True)

            if op_info["evento_quorum"].is_set() and op_info["resultado_final"] is not None:
                num_reqs, total_sum = op_info["resultado_final"]
                try:
                    self.sock_cliente.sendto(
                        protocolo.pack_ack(id_cliente, id_req, num_reqs, total_sum), addr
                    )
                except OSError:
                    pass
                return

            if conduzir_multicast:
                pacote_mc = protocolo.pack_order_multicast(seq_id, id_cliente, id_req, valor)
                self.multicast_rms(pacote_mc)

                tentativas = 0
                try:
                    while not op_info["evento_quorum"].wait(timeout=0.010):
                        if not check_lider():
                            return
                        tentativas += 1
                        self.multicast_rms(pacote_mc, reenvio=True)
                        if tentativas >= 200:
                            return
                finally:
                    with self.lock_fe:
                        op_info["em_andamento"] = False
            else:
                op_info["evento_quorum"].wait(timeout=0.25)

            if op_info["evento_quorum"].is_set() and op_info["resultado_final"] is not None:
                num_reqs, total_sum = op_info["resultado_final"]
                try:
                    self.sock_cliente.sendto(
                        protocolo.pack_ack(id_cliente, id_req, num_reqs, total_sum), addr
                    )
                except OSError:
                    pass
        except Exception:
            return

    def tratar_rm_ack(self, dados):
        try:
            seq_id, id_cliente, id_req, num_reqs, total_sum, rm_id = (
                protocolo.unpack_rm_ack(dados)
            )
        except Exception:
            return

        with self.lock_fe:
            if rm_id == self.node_id or rm_id not in self.nos_cluster:
                return

            self.ultimo_contato_rm[rm_id] = time.monotonic()
            self.aguardando_desde[rm_id] = None
            self._marcar_rm(rm_id, True)

            op_info = self.operacoes.get(seq_id)
            if op_info is None or op_info["evento_quorum"].is_set():
                return

            if rm_id in op_info["rms_que_votaram"]:
                self._verificar_quorum_op(seq_id, op_info)
                return
            op_info["rms_que_votaram"].add(rm_id)

            chave_res = (num_reqs, total_sum)
            grupo_votos = op_info["votos_concordantes"].setdefault(chave_res, set())
            grupo_votos.add(rm_id)

            self._verificar_quorum_op(seq_id, op_info)

    def _podar_historico(self):
        if self.max_historico is None:
            return
        while len(self.operacoes) > self.max_historico:
            oldest_seq = next(iter(self.operacoes))
            old_info = self.operacoes[oldest_seq]
            if old_info["resultado_final"] is None:
                break
            self.operacoes.pop(oldest_seq)
            old_key = old_info["dados_op"][:2]
            if self.op_to_seq.get(old_key) == oldest_seq:
                del self.op_to_seq[old_key]

    def _estado_sincronizacao_interno(self):
        if self.pendentes:
            sync_seq = min(self.pendentes)
            if sync_seq == 1:
                sync_num_reqs = 0
                sync_total_sum = 0
            else:
                prev_op = self.operacoes.get(sync_seq - 1)
                if prev_op is not None and prev_op["resultado_final"] is not None:
                    sync_num_reqs, sync_total_sum = prev_op["resultado_final"]
                else:
                    sync_num_reqs, sync_total_sum = self.num_reqs, self.total_sum
        else:
            sync_seq = self.next_seq
            sync_num_reqs = self.num_reqs
            sync_total_sum = self.total_sum
        return sync_seq, sync_num_reqs, sync_total_sum

    def _pacotes_pendentes_interno(self):
        pacotes = []
        for s in sorted(self.pendentes):
            op_info = self.operacoes.get(s)
            if op_info is not None and not op_info["evento_quorum"].is_set():
                c_id, r_id, val = op_info["dados_op"]
                pacotes.append(protocolo.pack_order_multicast(s, c_id, r_id, val))
        return pacotes

    def obter_estado_sincronizacao(self):
        with self.lock_fe:
            return self._estado_sincronizacao_interno()

    def obter_ultimo_seq_confirmado(self):
        with self.lock_fe:
            sync_seq, _, _ = self._estado_sincronizacao_interno()
            return sync_seq - 1

    def obter_pacotes_pendentes(self):
        with self.lock_fe:
            return self._pacotes_pendentes_interno()

    def reintegrar_rm(self, rm_id, ip_rm, port_cluster):
        with self.lock_fe:
            self.nos_cluster[rm_id] = (ip_rm, port_cluster)
            if self.seq_sem_estado is not None:
                return False, None, []  # o RM atrasado espera a 1ª confirmação
            alterou = self._marcar_rm(rm_id, True)
            estado_sync = self._estado_sincronizacao_interno()
            pendentes = self._pacotes_pendentes_interno()
            return alterou, estado_sync, pendentes

    def _verificar_quorum_op(self, seq_id, op_info):
        quorum = self.calcular_quorum()
        if quorum is None:
            return
        for chave_res, grupo_votos in op_info["votos_concordantes"].items():
            if len(grupo_votos & self.rms_ativos) >= quorum:
                op_info["resultado_final"] = chave_res
                op_info["evento_quorum"].set()
                self.pendentes.discard(seq_id)
                votos_divergentes = op_info["rms_que_votaram"] - grupo_votos
                op_info["rms_que_votaram"].clear()
                op_info["votos_concordantes"].clear()
                if chave_res[0] > self.num_reqs:
                    self.num_reqs, self.total_sum = chave_res
                if seq_id == self.seq_sem_estado:
                    self.seq_sem_estado = None  # totais agora vêm dos RMs em dia
                self._podar_historico()

                for rm_id in votos_divergentes:
                    addr_rm = self.nos_cluster.get(rm_id)
                    if addr_rm is not None:
                        try:
                            self.sock_cluster.sendto(
                                protocolo.pack_state_sync(
                                    seq_id + 1, self.num_reqs, self.total_sum
                                ),
                                addr_rm,
                            )
                        except OSError:
                            pass
                return

    def tratar_nack(self, dados, addr_remetente):
        """Reenvia operações faltantes a um RM que detectou lacuna de sequência."""
        try:
            expected_seq, rm_id = protocolo.unpack_nack(dados)
        except Exception:
            return

        pacotes_reenvio = []
        pacote_sync = None
        with self.lock_fe:
            if rm_id == self.node_id:
                return
            self.ultimo_contato_rm[rm_id] = time.monotonic()
            self.aguardando_desde[rm_id] = None
            destino = self.nos_cluster.get(rm_id, addr_remetente)

            if expected_seq < self.next_seq:
                if (self.next_seq - expected_seq) > 64 or expected_seq not in self.operacoes:
                    if self.seq_sem_estado is not None:
                        return  # sem totais confiáveis ainda: o RM pede de novo depois
                    sync_seq, sync_nr, sync_ts = self._estado_sincronizacao_interno()
                    pacote_sync = protocolo.pack_state_sync(sync_seq, sync_nr, sync_ts)
                    inicio = sync_seq
                else:
                    inicio = expected_seq

                for s in range(inicio, self.next_seq):
                    op_info = self.operacoes.get(s)
                    if op_info is not None:
                        c_id, r_id, val = op_info["dados_op"]
                        pacotes_reenvio.append(
                            protocolo.pack_order_multicast(s, c_id, r_id, val)
                        )

        if pacote_sync is not None:
            try:
                self.sock_cluster.sendto(pacote_sync, destino)
            except OSError:
                pass
        for pct in pacotes_reenvio:
            try:
                self.sock_cluster.sendto(pct, destino)
            except OSError:
                pass

    def verificar_timeouts_rms(self, timeout_rm=2.0):
        agora = time.monotonic()
        alterou = False
        with self.lock_fe:
            falhos = [
                rm_id
                for rm_id in list(self.rms_ativos)
                if self.aguardando_desde.get(rm_id) is not None
                and (agora - self.aguardando_desde[rm_id]) > timeout_rm
            ]
            for rm_id in falhos:
                if self._marcar_rm(rm_id, False):
                    alterou = True
        return alterou

    def adicionar_rm(self, rm_id, ip_rm, port_cluster):
        with self.lock_fe:
            self.nos_cluster[rm_id] = (ip_rm, port_cluster)
            return self._marcar_rm(rm_id, True)

    def obter_ips_cluster(self):
        with self.lock_fe:
            ips = [self.ip_maq]
            for rm_id in sorted(self.rms_ativos):
                addr = self.nos_cluster.get(rm_id)
                if addr is not None:
                    ips.append(addr[0])
            return ips

    def marcar_rm(self, rm_id, ativo):
        with self.lock_fe:
            return self._marcar_rm(rm_id, ativo)

    def _marcar_rm(self, rm_id, ativo):
        if rm_id == self.node_id or (rm_id in self.rms_ativos) == ativo:
            return False
        if ativo:
            self.rms_ativos.add(rm_id)
            self.ultimo_contato_rm[rm_id] = time.monotonic()
            self.aguardando_desde.setdefault(rm_id, None)
            evento, cor = "Ingresso/retorno do", protocolo.COLOR_FE
        else:
            self.rms_ativos.discard(rm_id)
            self.aguardando_desde[rm_id] = None
            for seq_id in sorted(self.pendentes):
                op_info = self.operacoes.get(seq_id)
                if op_info is not None and not op_info["evento_quorum"].is_set():
                    if self.rms_ativos:
                        self._verificar_quorum_op(seq_id, op_info)
            evento, cor = "Falha detectada no", protocolo.COLOR_FAIL
        ip_rm = self.nos_cluster.get(rm_id, (str(rm_id),))[0]
        print(
            protocolo.colorir(
                f"[FE {self.ip_maq}] {evento} RM {ip_rm} | Quórum atual: {self.formatar_quorum()}",
                cor,
            ),
            flush=True,
        )
        return True