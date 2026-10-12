# Como testar a Etapa 1

### Testes unitários automatizados
Para rodar todos os testes unitários da pasta `tests`:
```bash
python3 -m unittest discover tests
```
Ou executar os arquivos individualmente:
```bash
python3 tests/test_protocolo.py
python3 tests/test_servidor.py
```

### Teste de carga e desempenho
Com o servidor rodando em um terminal (ex: `python3 servidor.py 4000`), execute em outro terminal:
```bash
# Envia 10.000 requisições seguidas e mede a vazão
python3 tests/teste_carga.py 4000 10000
```

---

### Teste manual com cliente e servidor
Abra dois terminais dentro da pasta `Etapa1`.

#### 1. Iniciar o servidor
No primeiro terminal, execute o servidor informando a porta:
```bash
python3 servidor.py 4000
```
Ele deve iniciar e mostrar a linha inicial com os contadores zerados:
```
2026-09-29 09:00:00 num_reqs 0 total_sum 0
```

#### 2. Iniciar o cliente
No segundo terminal, execute o cliente usando a mesma porta:
```bash
python3 cliente.py 4000
```
O cliente vai procurar o servidor via broadcast e mostrar o IP encontrado:
```
2026-09-29 09:00:01 server_addr 127.0.0.1
```

#### 3. Testar o envio e a soma
No terminal do cliente, digite um número inteiro e aperte ENTER.

Exemplo:
```
10
```
O cliente vai mandar a requisição e mostrar a resposta:
```
2026-09-29 09:00:02 server 127.0.0.1 id_req 1 value 10 num_reqs 1 total_sum 10
```
E no terminal do servidor vai aparecer o registro do processamento da requisição 1.

Se você digitar outro valor (por exemplo `20`), a soma acumulada vai para 30.

#### Teste rápido com pipe
Para mandar vários números seguidos sem precisar digitar um por um:
```bash
printf "10\n20\n30\n" | python3 cliente.py 4000
```

#### Encerrar
Para finalizar o cliente ou o servidor, aperte `CTRL+C` (ou `CTRL+D` no cliente).

---

### Teste de Múltiplos Clientes no Docker (Recomendado)

Para simular um ambiente distribuído real onde cada cliente possui um **endereço IP distinto**, execute o script automatizado na raiz do projeto:

```bash
# Executa 3 clientes concorrentes com 500 requisições cada (validação rápida):
python test_docker.py -c 3 -r 500

# Executa 5 clientes concorrentes com 1.000 requisições cada:
python test_docker.py -c 5 -r 1000

# Executa usando os arquivos inteiros (1 milhão de números cada):
python test_docker.py -c 3 -r 0
```

O script:
1. Constrói a imagem Docker.
2. Cria a rede Docker bridge com IPs estáticos dedicados (`172.28.0.2` para o servidor, `172.28.0.11+` para clientes).
3. Dispara os clientes simultaneamente usando `RAND_NUM_1.txt`, `RAND_NUM_2.txt`, etc.
4. Valida se a soma e o total de requisições no servidor batem 100% com o esperado.
5. Limpa automaticamente todos os contêineres e redes.

---

### Execução com Docker Compose

Você também pode utilizar o `docker compose` diretamente:

```bash
# Subir o servidor e 3 clientes pré-configurados:
docker compose up --build

# Subir com limite de requisições por cliente (ex.: 500):
MAX_REQS=500 docker compose up --build

# Subir cliente interativo no terminal:
docker compose run cliente_interativo

# Finalizar e limpar contêineres:
docker compose down
```