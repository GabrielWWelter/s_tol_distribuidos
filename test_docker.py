import argparse
import os
import subprocess
import sys
import time
import re
import json
from concurrent.futures import ThreadPoolExecutor

NETWORK_NAME = "sisop2_test_net"
SERVER_CONTAINER = "sisop2_server"
IMAGE_NAME = "sisop2-etapa1:latest"

def run_cmd(cmd, capture_output=True, check=True):
    """Executa um comando no shell do sistema."""
    res = subprocess.run(cmd, shell=True, text=True, capture_output=capture_output)
    if check and res.returncode != 0:
        err = res.stderr.strip() if res.stderr else res.stdout.strip()
        raise RuntimeError(f"Comando falhou (código {res.returncode}): {cmd}\nErro: {err}")
    return res

def get_container_info(container_name):
    """Obtém os dados do contêiner em formato JSON usando docker inspect."""
    res = run_cmd(f"docker inspect {container_name}", check=False)
    if res.returncode != 0 or not res.stdout.strip():
        return None
    try:
        dados = json.loads(res.stdout)
        if dados and isinstance(dados, list):
            return dados[0]
    except Exception:
        pass
    return None

def cleanup_docker(num_clientes):
    """Limpa contêineres e rede do teste."""
    print("\n[+] Limpando contêineres e rede Docker...")
    # Remove servidor
    run_cmd(f"docker rm -f {SERVER_CONTAINER}", check=False)
    # Remove clientes
    for i in range(1, num_clientes + 1):
        run_cmd(f"docker rm -f sisop2_client_{i}", check=False)
    # Remove rede
    run_cmd(f"docker network rm {NETWORK_NAME}", check=False)
    print("[+] Limpeza concluída.")

def salvar_logs_docker(logs_dir, num_clientes, relatorio_txt=""):
    """Salva os logs do servidor e de todos os clientes em disco antes da limpeza."""
    os.makedirs(logs_dir, exist_ok=True)
    
    # 1. Log do Servidor
    res_srv = run_cmd(f"docker logs {SERVER_CONTAINER}", check=False)
    servidor_log_path = os.path.join(logs_dir, "servidor.log")
    with open(servidor_log_path, "w", encoding="utf-8") as f:
        f.write(res_srv.stdout)
    linhas_srv = len(res_srv.stdout.splitlines())

    # 2. Logs dos Clientes
    for i in range(1, num_clientes + 1):
        cname = f"sisop2_client_{i}"
        res_cli = run_cmd(f"docker logs {cname}", check=False)
        cli_log_path = os.path.join(logs_dir, f"cliente_{i}.log")
        with open(cli_log_path, "w", encoding="utf-8") as f:
            f.write(res_cli.stdout)

    # 3. Relatório do Teste
    if relatorio_txt:
        relatorio_path = os.path.join(logs_dir, "relatorio_teste.txt")
        with open(relatorio_path, "w", encoding="utf-8") as f:
            f.write(relatorio_txt)

    print(f"\n[+] Logs gerados e salvos com sucesso na pasta '{logs_dir}/':")
    print(f"    - {servidor_log_path} ({linhas_srv} linhas)")
    print(f"    - {logs_dir}/cliente_1.log a {logs_dir}/cliente_{num_clientes}.log")
    if relatorio_txt:
        print(f"    - {logs_dir}/relatorio_teste.txt")

def calcular_esperado_arquivo(caminho_arquivo, max_reqs):
    """Calcula a quantidade de requisições e a soma exata de um arquivo de teste."""
    soma = 0
    qtd = 0
    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
    
    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        for linha in f:
            if max_reqs > 0 and qtd >= max_reqs:
                break
            linha = linha.strip()
            if linha:
                try:
                    val = int(linha)
                    if val > 0:
                        soma = (soma + val) & 0xFFFFFFFFFFFFFFFF
                        qtd += 1
                except ValueError:
                    pass
    return qtd, soma

def main():
    parser = argparse.ArgumentParser(description="Teste automatizado de múltiplos clientes no Docker")
    parser.add_argument("-c", "--clientes", type=int, default=3, help="Quantidade de clientes simultâneos (padrão: 3)")
    parser.add_argument("-r", "--reqs", type=int, default=1000, help="Requisições por cliente (padrão: 1000, 0 para arquivo inteiro)")
    parser.add_argument("-p", "--porta", type=int, default=4000, help="Porta UDP do servidor (padrão: 4000)")
    parser.add_argument("--no-build", action="store_true", help="Pular a etapa de build da imagem Docker")
    parser.add_argument("-v", "--verbose", action="store_true", help="Exibir logs detalhados dos clientes")
    parser.add_argument("--logs-dir", type=str, default="logs", help="Diretório onde os arquivos de log serão salvos (padrão: logs)")
    parser.add_argument("--no-logs", action="store_true", help="Desativar a gravação de arquivos de log em disco")
    args = parser.parse_args()

    num_clientes = args.clientes
    reqs_por_cliente = args.reqs
    porta = args.porta

    print("=" * 65)
    print("  TESTE DE MÚLTIPLOS CLIENTES NO DOCKER - SISOP2")
    print("=" * 65)
    print(f"  Clientes concorrentes:  {num_clientes}")
    print(f"  Requisições por cliente:{reqs_por_cliente if reqs_por_cliente > 0 else 'Tudo'}")
    print(f"  Total previsto de reqs: {num_clientes * reqs_por_cliente if reqs_por_cliente > 0 else 'Total dos arquivos'}")
    print(f"  Porta do servidor UDP:  {porta}")
    print("=" * 65)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    etapa1_dir = os.path.join(base_dir, "Etapa1")

    # Arquivos de teste disponíveis
    arquivos_disponiveis = [
        "RAND_NUM_1.txt",
        "RAND_NUM_2.txt",
        "RAND_NUM_3.txt"
    ]

    # Calcula os valores esperados para cada cliente
    cliente_configs = []
    soma_esperada_total = 0
    reqs_esperadas_total = 0

    for i in range(1, num_clientes + 1):
        arq_nome = arquivos_disponiveis[(i - 1) % len(arquivos_disponiveis)]
        arq_path = os.path.join(etapa1_dir, arq_nome)
        qtd, soma = calcular_esperado_arquivo(arq_path, reqs_por_cliente)
        cliente_configs.append({
            "id": i,
            "arquivo": arq_nome,
            "reqs": qtd,
            "soma": soma
        })
        soma_esperada_total = (soma_esperada_total + soma) & 0xFFFFFFFFFFFFFFFF
        reqs_esperadas_total += qtd

    try:
        # Limpeza prévia de eventuais sobras
        cleanup_docker(num_clientes)

        # Build da Imagem Docker
        if not args.no_build:
            print(f"\n[1/5] Construindo a imagem Docker '{IMAGE_NAME}'...")
            run_cmd(f"docker build -t {IMAGE_NAME} -f Dockerfile .")
            print("[+] Imagem construída com sucesso.")
        else:
            print(f"\n[1/5] Pular build da imagem ('--no-build' ativo).")

        # Criação da Rede Docker Bridge com sub-rede dedicada
        print(f"\n[2/5] Criando rede Docker '{NETWORK_NAME}' (sub-rede 172.28.0.0/16)...")
        run_cmd(f"docker network create --subnet=172.28.0.0/16 {NETWORK_NAME}")
        print("[+] Rede Docker criada com suporte a broadcast UDP e IPs estáticos.")

        # Iniciar Contêiner do Servidor
        server_static_ip = "172.28.0.2"
        print(f"\n[3/5] Iniciando contêiner do servidor ({SERVER_CONTAINER}, IP {server_static_ip})...")
        run_cmd(f"docker run -d --name {SERVER_CONTAINER} --network {NETWORK_NAME} --ip {server_static_ip} {IMAGE_NAME} python3 servidor.py {porta}")
        time.sleep(1.5)

        # Verifica se o servidor subiu
        info_srv = get_container_info(SERVER_CONTAINER)
        if not info_srv or not info_srv.get("State", {}).get("Running"):
            logs = run_cmd(f"docker logs {SERVER_CONTAINER}", check=False).stdout
            raise RuntimeError(f"Servidor não conseguiu iniciar! Logs:\n{logs}")
        
        # Pega IP do servidor na rede
        networks = info_srv.get("NetworkSettings", {}).get("Networks", {})
        server_ip = next((net.get("IPAddress") for net in networks.values() if net.get("IPAddress")), server_static_ip)
        print(f"[+] Servidor rodando no IP interno: {server_ip} (porta {porta})")

        # Disparar Clientes Concorrentes
        print(f"\n[4/5] Disparando {num_clientes} clientes concorrentes no Docker...")
        inicio_exec = time.time()

        def iniciar_cliente(cfg):
            cid = cfg["id"]
            arq = cfg["arquivo"]
            cname = f"sisop2_client_{cid}"
            client_ip = f"172.28.0.{10 + cid}"
            cmd_docker = (
                f"docker run -d --name {cname} --network {NETWORK_NAME} --ip {client_ip} {IMAGE_NAME} "
                f"python3 cliente.py {porta} {arq} {reqs_por_cliente}"
            )
            run_cmd(cmd_docker)
            print(f"  -> Cliente {cid} iniciado no contêiner '{cname}' (IP: {client_ip}, fonte: {arq}, {cfg['reqs']} reqs)")

        with ThreadPoolExecutor(max_workers=min(num_clientes, 10)) as executor:
            list(executor.map(iniciar_cliente, cliente_configs))

        # Aguardar Conclusão de Todos os Clientes
        print("\n[+] Aguardando todos os clientes finalizarem suas requisições...")
        clientes_ativos = [f"sisop2_client_{cfg['id']}" for cfg in cliente_configs]
        
        timeout_max = max(60, num_clientes * (reqs_por_cliente // 50 + 5))
        tempo_inicio_espera = time.time()

        while clientes_ativos:
            if time.time() - tempo_inicio_espera > timeout_max:
                raise TimeoutError(f"Tempo limite excedido ({timeout_max}s) aguardando clientes!")
            
            for cname in list(clientes_ativos):
                info_cli = get_container_info(cname)
                if info_cli and not info_cli.get("State", {}).get("Running"):
                    # Contêiner finalizou
                    clientes_ativos.remove(cname)
                    # Checa código de saída
                    exit_code = info_cli.get("State", {}).get("ExitCode", 0)
                    if exit_code != 0:
                        logs = run_cmd(f"docker logs {cname}", check=False).stdout
                        print(f"[!] AVISO: {cname} terminou com erro (código {exit_code}):\n{logs}")
            
            if clientes_ativos:
                time.sleep(0.5)

        tempo_total = time.time() - inicio_exec
        print(f"[+] Todos os clientes concluíram em {tempo_total:.2f} segundos!")

        # Validar Resultados com o Servidor
        print(f"\n[5/5] Coletando e validando dados com o servidor...")
        server_logs = run_cmd(f"docker logs {SERVER_CONTAINER}").stdout.strip().splitlines()

        # Extrai contadores finais do log do servidor
        ultimo_num_reqs = 0
        ultimo_total_sum = 0
        ips_detectados = set()

        for linha in server_logs:
            match = re.search(r"client\s+([\d\.]+).*num_reqs\s+(\d+)\s+total_sum\s+(\d+)", linha)
            if match:
                ips_detectados.add(match.group(1))
                ultimo_num_reqs = int(match.group(2))
                ultimo_total_sum = int(match.group(3))

        vazao = (ultimo_num_reqs / tempo_total) if tempo_total > 0 else 0

        relatorio_txt = "\n".join([
            "=" * 65,
            "                  RELATÓRIO DE RESULTADOS",
            "=" * 65,
            f"  IPs distintos detectados pelo servidor: {len(ips_detectados)} {sorted(list(ips_detectados))}",
            f"  Tempo total de execução:               {tempo_total:.2f} s",
            f"  Vazão média agregada:                  {vazao:.1f} req/s",
            "-" * 65,
            f"  Requisições esperadas:                 {reqs_esperadas_total}",
            f"  Requisições processadas no servidor:   {ultimo_num_reqs}",
            "-" * 65,
            f"  Soma total esperada:                   {soma_esperada_total}",
            f"  Soma total obtida no servidor:         {ultimo_total_sum}",
            "=" * 65
        ])
        print("\n" + relatorio_txt)

        if not args.no_logs:
            salvar_logs_docker(args.logs_dir, num_clientes, relatorio_txt)

        if args.verbose:
            print("\n--- Amostra dos logs dos clientes ---")
            for cfg in cliente_configs:
                cname = f"sisop2_client_{cfg['id']}"
                logs = run_cmd(f"docker logs --tail 3 {cname}").stdout.strip()
                print(f"[{cname}]:\n{logs}\n")

        # Validações Finais
        erros = []
        if len(ips_detectados) != num_clientes:
            erros.append(f"Esperado {num_clientes} IPs distintos, mas o servidor detectou {len(ips_detectados)}.")

        if ultimo_num_reqs != reqs_esperadas_total:
            erros.append(f"Divergência de requisições: esperado {reqs_esperadas_total}, obtido {ultimo_num_reqs}.")

        if ultimo_total_sum != soma_esperada_total:
            erros.append(f"Divergência de soma: esperado {soma_esperada_total}, obtido {ultimo_total_sum}.")

        if erros:
            print("\n[FALHA] Foram encontradas divergências no teste:")
            for e in erros:
                print(f"  - {e}")
            sys.exit(1)
        else:
            print("\n[OK] TESTE CONCLUÍDO COM SUCESSO!")
            print("     Todos os clientes executaram concorrentemente com IPs próprios.")
            print("     As somas e requisições bateram 100% com o esperado!")

    except Exception as e:
        print(f"\n[ERRO] Ocorreu uma falha durante o teste: {e}")
        try:
            if not args.no_logs:
                salvar_logs_docker(args.logs_dir, num_clientes, f"Falha no teste: {e}")
        except Exception:
            pass
        sys.exit(1)
    finally:
        cleanup_docker(num_clientes)

if __name__ == "__main__":
    main()
