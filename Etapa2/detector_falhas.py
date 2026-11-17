import threading
import time


class DetectorFalhas:
    def __init__(self, timeout, relogio=time.monotonic):
        self.timeout = timeout
        self.relogio = relogio
        self.ultimo_heartbeat = {}
        self.conhecidos = set()
        self.lock = threading.Lock()

    def registrar(self, no_id):
        with self.lock:
            if no_id in self.ultimo_heartbeat:
                evento = None
            elif no_id in self.conhecidos:
                evento = "retorno"
            else:
                evento = "ingresso"
            self.ultimo_heartbeat[no_id] = self.relogio()
            self.conhecidos.add(no_id)
            return evento

    def verificar(self):
        agora = self.relogio()
        with self.lock:
            falhos = [
                no_id
                for no_id, instante in self.ultimo_heartbeat.items()
                if agora - instante > self.timeout
            ]
            for no_id in falhos:
                del self.ultimo_heartbeat[no_id]
            return falhos

    def ativos(self):
        with self.lock:
            return set(self.ultimo_heartbeat)
