import sys
import os
import time
import socket
import random

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import protocolo

def main():
    porta = 4000
    if len(sys.argv) > 1:
        porta = int(sys.argv[1])

    total_requisicoes = 10000
    if len(sys.argv) > 2:
        total_requisicoes = int(sys.argv[2])

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2.0)
    server_addr = ('127.0.0.1', porta)

    # Consulta o estado atual do servidor para este cliente (id_req=0 para nao alterar nada)
    sock.sendto(protocolo.pack_req(0, 0), server_addr)
    resp, _ = sock.recvfrom(1024)
    last_req_ini, num_reqs_ini, total_sum_ini = protocolo.unpack_ack(resp)

    start_id = last_req_ini + 1
    end_id = start_id + total_requisicoes - 1

    if last_req_ini > 0:
        print(f"Servidor já possui histórico para este IP: last_req={last_req_ini}, soma_acumulada={total_sum_ini}")
        print(f"Continuando envio a partir de id_req={start_id} até {end_id}...")
    else:
        print(f"Enviando {total_requisicoes} requisições (id_req {start_id} a {end_id}) para 127.0.0.1:{porta}...")

    soma_esperada = total_sum_ini
    soma_novos_valores = 0
    inicio = time.time()

    for i in range(start_id, end_id + 1):
        valor = random.randint(1, 1000)
        soma_novos_valores += valor
        soma_esperada = (soma_esperada + valor) & 0xFFFFFFFFFFFFFFFF
        pacote = protocolo.pack_req(i, valor)
        sock.sendto(pacote, server_addr)
        resp, _ = sock.recvfrom(1024)

    fim = time.time()
    duracao = fim - inicio
    vazao = total_requisicoes / duracao

    id_req, num_reqs, total_sum = protocolo.unpack_ack(resp)
    sock.close()

    print(f"\n--- Resultado do Teste de Carga ---")
    print(f"Requisições enviadas nesta execução: {total_requisicoes}")
    print(f"Tempo total:                         {duracao:.2f} s")
    print(f"Vazão média:                         {vazao:.0f} req/s")
    print(f"Soma dos novos valores gerados:      {soma_novos_valores}")
    print(f"Soma total esperada no servidor:     {soma_esperada}")
    print(f"Soma total obtida no servidor:       {total_sum}")

    if total_sum == soma_esperada and num_reqs == (num_reqs_ini + total_requisicoes):
        print("\n[OK] SOMA CORRETA: O servidor somou todas as novas requisições perfeitamente!")
    else:
        print(f"\n[ERRO] Divergência! Esperado: {soma_esperada}, Obtido: {total_sum}")
        sys.exit(1)

if __name__ == "__main__":
    main()
