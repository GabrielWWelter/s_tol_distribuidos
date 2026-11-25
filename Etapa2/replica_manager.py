import threading
import protocolo
from buffer_reordenacao import (
    BufferReordenacao,
    ConflitoSequenciaError,
    OperacaoJaSequenciadaError,
)


class ReplicaManager:
    def __init__(self, ip_maq, node_id, nos_cluster, sock_cluster):
        self.ip_maq = ip_maq
        self.node_id = node_id
        self.nos_cluster = nos_cluster
        self.sock_cluster = sock_cluster

        # Instancia um buffer para cada réplica
        self.buffer = BufferReordenacao()
        self.lock_rm = threading.Lock()

    def obter_proximo_seq(self):
        """Retorna o próximo seq_id esperado para continuidade caso vire FE"""
        with self.lock_rm:
            return self.buffer.expected_seq

    def sincronizar(self, next_seq, num_reqs, total_sum, leader_id=None):
        with self.lock_rm:
            if (
                self.buffer.expected_seq == next_seq
                and self.buffer.num_reqs == num_reqs
                and self.buffer.total_sum == total_sum
                and next_seq not in self.buffer.pendencias
            ):
                return
            aplicadas = self.buffer.sincronizar_estado(next_seq, num_reqs, total_sum)
        print(
            protocolo.colorir(
                f"[RM {self.ip_maq}] Sincronizado com cluster: seq={next_seq} | num_reqs={num_reqs} | total_sum={total_sum}",
                protocolo.COLOR_RM,
            ),
            flush=True,
        )
        if aplicadas and leader_id:
            fe_addr = self.nos_cluster.get(leader_id)
            if fe_addr:
                for s, c_id, r_id, n_reqs, t_sum in aplicadas:
                    pacote_ack = protocolo.pack_rm_ack(
                        s, c_id, r_id, n_reqs, t_sum, self.node_id
                    )
                    try:
                        self.sock_cluster.sendto(pacote_ack, fe_addr)
                    except OSError:
                        pass

    def reenviar_operacoes(self, dados, addr):
        """Atende o NACK de um novo líder atrasado com operações já aplicadas."""
        inicio, _ = protocolo.unpack_nack(dados)
        with self.lock_rm:
            fim = min(inicio + 64, self.buffer.expected_seq)
            pacotes = [
                protocolo.pack_order_multicast(s, *self.buffer.historico[s][0])
                for s in range(inicio, fim)
                if s in self.buffer.historico
            ]
        for pacote in pacotes:
            try:
                self.sock_cluster.sendto(pacote, addr)
            except OSError:
                pass

    def tratar_order_multicast(self, dados, addr_remetente, leader_id):
        try:
            seq_id, id_cliente, id_req, valor = protocolo.unpack_order_multicast(dados)
        except Exception:
            return

        with self.lock_rm:
            if seq_id >= self.buffer.expected_seq and seq_id not in self.buffer.pendencias:
                print(
                    protocolo.colorir(
                        f"[RM {self.ip_maq}] Requisição recebida do FE: seq={seq_id} "
                        f"(client={id_cliente}, req={id_req}, val={valor})",
                        protocolo.COLOR_RM,
                    ),
                    flush=True,
                )
            try:
                aplicou, resultados = self.buffer.processar(
                    seq_id, id_cliente, id_req, valor
                )
            except OperacaoJaSequenciadaError:
                aplicou = False
                resultados = self.buffer.resolver_duplicata_resequenciada(
                    seq_id, id_cliente, id_req
                )
            except (ConflitoSequenciaError, ValueError) as e:
                print(
                    protocolo.colorir(
                        f"[RM {self.ip_maq}] Erro de ordenação: {e}",
                        protocolo.COLOR_FAIL,
                    ),
                    flush=True,
                )
                return

        if not resultados:
            if seq_id < self.buffer.expected_seq:
                fe_addr = self.nos_cluster.get(leader_id, addr_remetente)
                pacote_ack = protocolo.pack_rm_ack(
                    seq_id, id_cliente, id_req, self.buffer.num_reqs, self.buffer.total_sum, self.node_id
                )
                try:
                    self.sock_cluster.sendto(pacote_ack, fe_addr)
                except OSError:
                    pass
            elif seq_id > self.buffer.expected_seq:
                print(
                    protocolo.colorir(
                        f"[RM {self.ip_maq}] seq={seq_id} retido (esperando seq={self.buffer.expected_seq})",
                        protocolo.COLOR_RM,
                    ),
                    flush=True,
                )
                fe_addr = self.nos_cluster.get(leader_id, addr_remetente)
                if fe_addr is not None:
                    try:
                        self.sock_cluster.sendto(
                            protocolo.pack_nack(self.buffer.expected_seq, self.node_id),
                            fe_addr,
                        )
                    except OSError:
                        pass
            return

        fe_addr = self.nos_cluster.get(leader_id, addr_remetente)
        for s, c_id, r_id, num_reqs, total_sum in resultados:
            if aplicou:
                print(
                    protocolo.colorir(
                        f"[RM {self.ip_maq}] Aplicado seq={s} | num_reqs={num_reqs} | total_sum={total_sum}",
                        protocolo.COLOR_RM,
                    ),
                    flush=True,
                )
            pacote_ack = protocolo.pack_rm_ack(
                s, c_id, r_id, num_reqs, total_sum, self.node_id
            )
            try:
                self.sock_cluster.sendto(pacote_ack, fe_addr)
            except OSError:
                pass