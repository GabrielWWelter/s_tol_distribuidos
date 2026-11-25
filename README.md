# Serviço Distribuído de Soma Tolerante a Falhas

Este repositório contém a implementação do trabalho prático da disciplina **INF01085 — Sistemas Distribuídos e Tolerantes a Falhas** (UFRGS).

A **Parte 2** estende o serviço desenvolvido na Parte 1, implementando replicação ativa determinística de máquinas de estados, ordenação total de mensagens, quórum por maioria simples, detecção de falhas e eleição de líder através do **Algoritmo do Anel**.

---

## 1. Requisitos do Sistema

- **Sistema Operacional:** Linux (testado em Ubuntu 22.04 LTS, x86_64);
- **Interpretador:** Python 3.10 ou superior (`python3`);
- **Utilitários:** `make`, `zip`.

---

## 2. Compilação e Validação Automatizada

Embora a linguagem Python seja interpretada, a compilação e validação sintática de todos os módulos (`.pyc`) é automatizada via `Makefile`:

```bash
make compile
# ou simplesmente:
make
```

Para limpar arquivos compilados (`__pycache__`) e temporários:
```bash
make clean
```

---

## 3. Execução dos Testes Automatizados

O projeto conta com suítes abrangentes de testes unitários automatizados com `unittest`:

```bash
# Executa os 80 testes unitários da Etapa 2
make test

# Executa todos os testes (Etapa 1 e Etapa 2)
make test-all
```

---

## 4. Tutorial de Execução Manual

### Cenário: Cluster com 3 Servidores e 1 Cliente

Abra quatro terminais no diretório raiz do projeto.

#### Passo 1: Iniciar os Servidores
Os servidores detectam automaticamente seus endereços IP e portas, não sendo necessário passar IPs via linha de comando. Na mesma máquina, instâncias subsequentes alocam automaticamente endereços de loopback adicionais (`127.0.0.1`, `127.0.0.2`, `127.0.0.3`):

- **Terminal 1 (Primeiro servidor — torna-se Front-End inicial):**
  ```bash
  python3 Etapa2/servidor.py 4000 5000
  ```
- **Terminal 2 (Segundo servidor — ingressa como Replica Manager):**
  ```bash
  python3 Etapa2/servidor.py 4000 5000
  ```
- **Terminal 3 (Terceiro servidor — ingressa como Replica Manager):**
  ```bash
  python3 Etapa2/servidor.py 4000 5000
  ```

#### Passo 2: Iniciar o Cliente
- **Terminal 4:**
  ```bash
  python3 Etapa2/cliente.py 4000
  ```
  O cliente executará a descoberta via broadcast, conectará ao Front-End ativo e exibirá o prompt de leitura.

#### Passo 3: Envio de Operações
No terminal do cliente, digite um número inteiro e pressione `Enter`:
```
10
```
O cliente emitirá a requisição e exibirá a resposta com o total consolidado:
```
server 127.0.0.1 id_req 1 value 10 num_reqs 1 total_sum 10
```
Digitando `20`, o acumulador será atualizado para `total_sum 30`.

Também é possível enviar um lote de valores via redirecionamento de entrada:
```bash
printf "10\n20\n30\n" | python3 Etapa2/cliente.py 4000
```

## 5. Empacotamento para Entrega

Para gerar o arquivo compactado `entrega_etapa2.zip` pronto para submissão:

```bash
make zip
```

---

## 6. Estrutura de Diretórios

```
.
├── Makefile                # Script automatizado de compilação, testes e execução
├── README.md               # Este tutorial de compilação e execução
├── Etapa1/                 # Código-fonte e testes da Parte 1
│   ├── cliente.py
│   ├── servidor.py
│   ├── protocolo.py
│   └── tests/
└── Etapa2/                 # Código-fonte e testes da Parte 2
    ├── cliente.py          # Cliente Stop-and-Wait com redescoberta sob failover
    ├── servidor.py         # Orquestrador dinâmico de papéis (FE / RM)
    ├── front_end.py        # Sequenciador Global, agregação de quórum e ACKs
    ├── replica_manager.py  # Máquina de estados replicada e execução ativa
    ├── buffer_reordenacao.py # Buffer de ordenação total e idempotência
    ├── eleicao_anel.py     # Algoritmo do Anel (Chang & Roberts)
    ├── detector_falhas.py  # Monitor de heartbeats e detecção de inatividade
    ├── protocolo.py        # Serialização binária das mensagens de controle
    ├── Relatório/          # Relatório técnico em LaTeX (main.tex)
    └── tests/              # 80 testes unitários automatizados
```