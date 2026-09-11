**Máximo de páginas:** 3 páginas

**Estrutura do Relatório:** O trabalho deve conter as seguintes seções:

## Seção 1: Contexto e Fronteira do Sistema
* Qual é o sistema sob análise?
* Qual é a fronteira do sistema adotada pelo grupo? O que é considerado componente interno e o que é ambiente externo?

## Seção 2: Cadeia Causal (Fault -> Error -> Failure)
Identifique com precisão cirúrgica:
* **Falta (Fault):** A causa primária/defeito no código, na configuração ou no componente físico.
* **Erro (Error):** O estado incorreto interno do sistema gerado pela falta antes de ser percebido pelo usuário.
* **Falha (Failure):** O evento no qual o serviço entregue pelo sistema desviou de sua especificação ou expectativa do usuário.

## Seção 3: Classificação Taxonômica da Falta Raiz
Com base no artigo de Avižienis, classifique a falta raiz quanto a:
* **Fase de criação:** Desenvolvimento (Development) ou Operacional (Operational)?
* **Fronteira:** Interna (Internal) ou Externa (External)?
* **Dimensão:** Hardware ou Software?
* **Intenção:** Acidental ou Deliberada?
* **Persistência:** Permanente ou Transitória?

## Seção 4: Atributos Afetados
Indique quais atributos de dependabilidade foram violados e justifique brevemente:
* **Availability** (Disponibilidade)
* **Reliability** (Confiabilidade)
* **Safety** (Segurança operacional / Ausência de catástrofes)
* **Integrity** (Integridade)
* **Maintainability** (Manutenibilidade)
