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

        self.nos_cluster = {ip_para_id(ip): (ip, port_cluster) for ip in ips_cluster}
        self.nos_cluster[self.node_id] = (ip_maq, port_cluster)
        self.leader_id = max(self.nos_cluster.keys())

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

    def iniciar(self):
        threading.Thread(target=self.loop_broadcast_cliente, daemon=True).start()
        threading.Thread(target=self.loop_canal_cliente, daemon=True).start()
        threading.Thread(target=self.monitorar_rms, daemon=True).start()

        ip_lider = self.nos_cluster[self.leader_id][0]
        if self.is_leader():
            print(
                protocolo.colorir(
                    f"[FE {self.ip_maq}] Ativo como Front-End | "
                    f"Quórum atual: {self.fe.formatar_quorum()}",
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
                elif tipo == protocolo.MSG_NACK and self.is_leader():
                    self.fe.tratar_nack(dados, addr)
                elif tipo == protocolo.MSG_DISCOVERY_FWD and self.is_leader():
                    try:
                        client_addr = protocolo.unpack_discovery_fwd(dados)
                        self.fe.responder_descoberta(client_addr)
                    except Exception:
                        pass
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