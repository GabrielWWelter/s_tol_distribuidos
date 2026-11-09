class BufferReordenacao:
    def __init__(self):
        self.expected_seq = 1
        self.num_reqs = 0
        self.total_sum = 0
        self.pendencias = {}

    def processar(self, seq_id, id_req, valor):
        # requisicao duplicada: retorna estado atual sem alterar acumulador
        if seq_id < self.expected_seq:
            return False, [(seq_id, id_req, self.num_reqs, self.total_sum)]

        # requisicao fora de ordem: armazena no buffer de pendencias
        if seq_id > self.expected_seq:
            self.pendencias[seq_id] = (id_req, valor)
            return False, []

        # requisicao esperada: aplica soma e desencadeia pendencias consecutivas
        aplicadas = []
        curr_seq = seq_id
        curr_id_req = id_req
        curr_valor = valor

        while True:
            self.num_reqs = (self.num_reqs + 1) & 0xFFFFFFFFFFFFFFFF
            self.total_sum = (self.total_sum + curr_valor) & 0xFFFFFFFFFFFFFFFF
            aplicadas.append((curr_seq, curr_id_req, self.num_reqs, self.total_sum))
            self.expected_seq += 1

            if self.expected_seq in self.pendencias:
                curr_seq = self.expected_seq
                curr_id_req, curr_valor = self.pendencias.pop(curr_seq)
            else:
                break

        return True, aplicadas
