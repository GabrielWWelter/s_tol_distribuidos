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

    def tratar_order_multicast(self, dados, addr_remetente, leader_id):
        try:
            seq_id, id_cliente, id_req, valor = protocolo.unpack_order_multicast(dados)
        except Exception:
            return

        with self.lock_rm:
            try:
                aplicou, resultados = self.buffer.processar(
                    seq_id, id_cliente, id_req, valor
                )
            except (ConflitoSequenciaError, OperacaoJaSequenciadaError, ValueError) as e:
                print(
                    protocolo.colorir(
                        f"[RM {self.ip_maq}] Erro de ordenação: {e}",
                        protocolo.COLOR_FAIL,
                    ),
                    flush=True,
                )
                return

        if not resultados:
            print(
                protocolo.colorir(
                    f"[RM {self.ip_maq}] seq={seq_id} retido (esperando seq={self.buffer.expected_seq})",
                    protocolo.COLOR_RM,
                ),
                flush=True,
            )
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