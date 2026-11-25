.PHONY: all compile test test-all server client clean report zip help

PYTHON ?= python3
CLIENT_PORT ?= 4000
SERVER_PORT ?= 5000

all: compile

compile:
	@echo "==> Compilando bytecodes Python e verificando sintaxe..."
	$(PYTHON) -m compileall -q $(wildcard Etapa2 Etapa1)
	@echo "==> Compilacao concluida com sucesso."

test:
	@echo "==> Executando testes unitarios da Etapa 2..."
	$(PYTHON) -m unittest discover -s Etapa2/tests -v

test-all:
	@echo "==> Executando todos os testes (Etapa 1 + Etapa 2)..."
	$(PYTHON) -m unittest discover -s Etapa2/tests -v
	@if [ -d Etapa1/tests ]; then $(PYTHON) -m unittest discover -s Etapa1/tests -v; fi

server:
	@echo "==> Iniciando instancia do Servidor (portas $(CLIENT_PORT) e $(SERVER_PORT))..."
	$(PYTHON) Etapa2/servidor.py $(CLIENT_PORT) $(SERVER_PORT)

client:
	@echo "==> Iniciando Cliente interativo (porta $(CLIENT_PORT))..."
	$(PYTHON) Etapa2/cliente.py $(CLIENT_PORT)

report:
	@echo "==> Compilando relatorio LaTeX..."
	@command -v pdflatex >/dev/null 2>&1 || { echo "ERRO: pdflatex nao encontrado. Instale o TeX Live."; exit 1; }
	@cd "Etapa2/Relatório" && pdflatex -interaction=nonstopmode -halt-on-error main.tex && pdflatex -interaction=nonstopmode -halt-on-error main.tex
	@echo "==> Relatorio compilado: Etapa2/Relatório/main.pdf"

clean:
	@echo "==> Limpando arquivos temporarios e caches..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	rm -f "Etapa2/Relatório"/*.aux "Etapa2/Relatório"/*.log "Etapa2/Relatório"/*.out "Etapa2/Relatório"/*.toc "Etapa2/Relatório"/*.synctex.gz 2>/dev/null || true
	rm -f entrega_etapa2.zip
	@echo "==> Limpeza concluida."

# o relatorio e recompilado antes; se falhar, o pacote nao e gerado
zip: clean report
	@echo "==> Gerando arquivo ZIP para entrega..."
	zip -r entrega_etapa2.zip Makefile README.md Etapa2 -x "*.git*" "*__pycache__*" "*.pyc" "Etapa2/erro.txt" "*.aux" "*.log" "*.out" "*.toc" "*.fls" "*.fdb_latexmk" "*.synctex.gz"
	@echo "==> Pacote gerado com sucesso: entrega_etapa2.zip"

help:
	@echo "Alvos disponiveis no Makefile:"
	@echo "  make compile   - Compila e valida a sintaxe dos arquivos Python"
	@echo "  make test      - Executa os 68 testes unitarios da Etapa 2"
	@echo "  make test-all  - Executa os testes da Etapa 2 (e da Etapa 1, se a pasta existir)"
	@echo "  make server    - Inicia uma instancia de servidor (portas 4000 5000)"
	@echo "  make client    - Inicia o cliente interativo (porta 4000)"
	@echo "  make report    - Compila o relatorio LaTeX em PDF (requer pdflatex)"
	@echo "  make clean     - Remove caches __pycache__ e arquivos temporarios"
	@echo "  make zip       - Gera o relatorio e o pacote ZIP de entrega (requer pdflatex)"
