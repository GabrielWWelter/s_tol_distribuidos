class ConflitoSequenciaError(ValueError):
    pass


class OperacaoJaSequenciadaError(ValueError):
    pass


class BufferReordenacao:
    def __init__(self):
        self.expected_seq = 1
        self.num_reqs = 0
        self.total_sum = 0
        self.pendencias = {}
        self.historico = {}
        self.seq_por_operacao = {}

    def processar(self, seq_id, client_id, id_req, valor):
        if seq_id < 1:
            raise ValueError("seq_id deve ser maior ou igual a 1")

        operacao = (client_id, id_req, valor)
        chave_operacao = (client_id, id_req)

        # Uma operação não pode receber dois números de sequência diferentes.
        seq_anterior = self.seq_por_operacao.get(chave_operacao)
        if seq_anterior is not None and seq_anterior != seq_id:
            raise OperacaoJaSequenciadaError(
                f"operação {chave_operacao} já associada a seq_id {seq_anterior}"
            )

        # Requisição duplicada: devolve o resultado original sem somar novamente.
        if seq_id < self.expected_seq:
            operacao_original, resultado_original = self.historico[seq_id]
            if operacao != operacao_original:
                raise ConflitoSequenciaError(
                    f"seq_id {seq_id} recebido com conteúdo diferente"
                )
            return False, [resultado_original]

        # Requisição fora de ordem: mantém a primeira cópia recebida.
        if seq_id > self.expected_seq:
            if seq_id in self.pendencias:
                if self.pendencias[seq_id] != operacao:
                    raise ConflitoSequenciaError(
                        f"seq_id {seq_id} recebido com conteúdo diferente"
                    )
                return False, []

            self.pendencias[seq_id] = operacao
            self.seq_por_operacao[chave_operacao] = seq_id
            return False, []

        # Requisição esperada: aplica soma e desencadeia pendências consecutivas.
        aplicadas = []
        curr_seq = seq_id
        curr_client_id = client_id
        curr_id_req = id_req
        curr_valor = valor

        while True:
            self.num_reqs = (self.num_reqs + 1) & 0xFFFFFFFFFFFFFFFF
            self.total_sum = (self.total_sum + curr_valor) & 0xFFFFFFFFFFFFFFFF
            resultado = (
                curr_seq,
                curr_client_id,
                curr_id_req,
                self.num_reqs,
                self.total_sum,
            )
            curr_operacao = (curr_client_id, curr_id_req, curr_valor)
            self.historico[curr_seq] = (curr_operacao, resultado)
            self.seq_por_operacao[(curr_client_id, curr_id_req)] = curr_seq
            aplicadas.append(resultado)
            self.expected_seq += 1

            if self.expected_seq in self.pendencias:
                curr_seq = self.expected_seq
                curr_client_id, curr_id_req, curr_valor = self.pendencias.pop(curr_seq)
            else:
                break

        return True, aplicadas
