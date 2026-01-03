
# ETL Pipeline & ERP Database Manager

Este projeto consiste em um sistema completo de **ETL (Extract, Transform, Load)** desenvolvido em **Python puro**. O script migra dados de arquivos legados (CSV) de um sistema ERP para um banco de dados relacional estruturado em **SQLite**.

O objetivo principal é demonstrar a manipulação de dados, garantia de integridade referencial e modelagem de banco de dados via código, sem dependência de bibliotecas externas pesadas (como Pandas), focando na lógica de programação e eficiência.

## Funcionalidades

* **Extração de Dados:** Leitura automatizada de múltiplos arquivos CSV (Representantes, Clientes, Produtos, Pedidos).
* **Limpeza e Transformação (Data Cleaning):**
    * Conversão segura de tipos (`String` -> `Int`/`Float`) com tratamento de erros.
    * Tratamento de formatação monetária (remoção de vírgulas, ajuste de decimais).
    * Aplicação de valores `DEFAULT` para campos nulos ou inválidos.
* **Integridade Referencial:** Validação lógica de **Foreign Keys (Chaves Estrangeiras)** em memória antes da inserção no banco (evita erros de *Constraint* e registros órfãos).
* **Modelagem de Banco de Dados:** Criação automática do Schema (DDL) com:
    * `PRIMARY KEY` (Simples e Composta).
    * `FOREIGN KEY` constraints.
    * `CHECK` constraints para validação de domínio (ex: UF com 2 caracteres, Tipos de Pessoa).

##  Estrutura do Banco de Dados

O banco de dados `bd_erp_n2.db` é criado automaticamente seguindo rigorosamente as regras de negócio:

* **Repres:** Cadastro de representantes comerciais e comissões.
* **FornClien:** Entidade unificada para Clientes e Fornecedores.
* **Produtos:** Catálogo de produtos com controle de estoque (`QTDEMIN`, `QTDEESTQ`) e custos.
* **Pedidos:** Cabeçalho dos pedidos de venda, incluindo validação de fluxo (`SITUACAO`, `FINALIDNFE`).
* **PedidosItem:** Detalhamento dos itens, utilizando **Chave Primária Composta** (`NUMPED` + `NUMITEM`).

## Tecnologias Utilizadas

* **Python 3.x**
* **SQLite3** (Banco de dados embarcado)
* **CSV** (Biblioteca padrão para parsing de arquivos)
* **Typing** (Para Type Hinting e clareza de código)

## Como Executar

1. **Pré-requisitos:** Certifique-se de ter o Python 3 instalado.
2. **Arquivos de Dados:** Coloque os arquivos CSV originais na mesma pasta do script:
    * `DadosERPRepres.csv`
    * `DadosERPFornClien.csv`
    * `DadosERPProdutos.csv`
    * `DadosERPPedidos.csv`
    * `DadosERPPedidosItem.csv`
3. **Execução:**
    ```bash
    python sistema_sgbd_completo.py
    ```
4. **Resultado:** O script gerará o arquivo `bd_erp_n2.db` e exibirá o log de processamento no terminal.

##  Aprendizados e Destaques

Este projeto foi desenvolvido para exercitar conceitos fundamentais de Engenharia de Dados e Banco de Dados:
* Tratamento de **PK Composta** em SQLite.
* Implementação manual de validação de FKs utilizando Estruturas de Dados (`Sets`) para performance O(1).
* Uso de **Transações Atômicas** (`conn.commit`) para garantir a consistência dos dados.

---

- Kauê Patrick

