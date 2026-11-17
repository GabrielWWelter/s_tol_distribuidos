import sys
import socket
import threading
import time
import ipaddress
import protocolo
from front_end import FrontEnd
from replica_manager import ReplicaManager


def ip_para_id(ip_str):
    return int(ipaddress.IPv4Address(ip_str))


class ServidorReplicado:
    def __init__(self, ip_maq, port_cliente, port_cluster, ips_cluster):
        self.ip_maq = ip_maq
        self.node_id = ip_para_id(ip_maq)
        self.port_cliente = port_cliente
        self.port_cluster = port_cluster
        self.ips_cluster = ips_cluster

        # Mapeia cada node_id para uma tupla (ip, port_cluster) de todos os participantes do cluster
        self.nos_cluster = {ip_para_id(ip): (ip, port_cluster) for ip in ips_cluster}
        # Elege como líder o nó de maior IP numérico
        self.leader_id = max(self.nos_cluster.keys())

        # Canal Externo = Clientes <-> FE
        self.sock_cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_cliente.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Possibilitar testes em uma mesma máquina (APAGAR DEPOIS)
        self.sock_cliente.bind((self.ip_maq, self.port_cliente))

        # Canal Intra-Cluster = FE <-> RM
        self.sock_cluster = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_cluster.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Possibilitar testes em uma mesma máquina (APAGAR DEPOIS)
        self.sock_cluster.bind((self.ip_maq, self.port_cluster))

        # Módulos especializados
        self.fe = FrontEnd(
            self.ip_maq,
            self.node_id,
            self.nos_cluster,
            self.leader_id,
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

    def loop_canal_cliente(self):
        """Escuta o canal externo e dispara uma thread por requisição no FE"""
        while True:
            try:
                dados, addr = self.sock_cliente.recvfrom(1024)
            except OSError:
                continue

            if not self.is_leader() or not dados:
                continue

            t = threading.Thread(
                target=self.fe.processar_requisicao_cliente,
                args=(dados, addr, self.is_leader),
                daemon=True,
            )
            t.start()

    def monitorar_rms(self):
        """Monitora periodicamente a vitalidade dos RMs quando atua como FE"""
        while True:
            time.sleep(0.5)
            if self.is_leader():
                self.fe.verificar_timeouts_rms()

    def iniciar(self):
        threading.Thread(target=self.loop_canal_cliente, daemon=True).start()
        threading.Thread(target=self.monitorar_rms, daemon=True).start()

        ip_lider = self.nos_cluster[self.leader_id][0]
        if self.is_leader():
            print(
                protocolo.colorir(
                    f"[FE {self.ip_maq}] Ativo como Front-End | "
                    f"Quórum atual: {self.fe.calcular_quorum()}/{len(self.fe.rms_ativos)}",
                    protocolo.COLOR_LEADER,
                ),
                flush=True,
            )
        else:
            print(
                protocolo.colorir(
                    f"[RM {self.ip_maq}] Ativo como Replica Manager (FE={ip_lider})",
                    protocolo.COLOR_RM,
                ),
                flush=True,
            )

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
        except KeyboardInterrupt:
            pass
        finally:
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