import csv
import sqlite3
from typing import Optional, Tuple, List, Dict, Set


# ======================================================================
# FUNÇÕES ETL (EXTRACT, TRANSFORM, LOAD)
# ======================================================================

def read_csv_file(filename: str, encoding: str = 'utf-8') -> Tuple[Optional[list], Optional[list]]:
    """
    Função genérica para ler um arquivo CSV.
    Retorna o cabeçalho (lista) e os dados (lista de listas).

    Args:
        filename: Nome do arquivo CSV
        encoding: Codificação do arquivo (padrão: utf-8)

    Returns:
        Tupla (header, data) ou (None, None) em caso de erro
    """
    try:
        with open(filename, mode='r', encoding=encoding) as f:
            reader = csv.reader(f)
            header = next(reader)  # lê a primeira linha (cabeçalho)
            data = [row for row in reader]  # lê todo o resto dos dados
            return header, data

    except FileNotFoundError:
        print(f"ERRO CRÍTICO: Arquivo '{filename}' não encontrado.")
        return None, None

    except Exception as e:
        print(f"Erro ao ler '{filename}': {e}")
        return None, None


def get_valid_pk_set(data: list, pk_column_index: int) -> Set[int]:
    """
    Recebe os dados de um CSV (lista de listas) e o índice da coluna da PK.
    Retorna um 'set' (conjunto) com todos os IDs de PK válidos (como inteiros).

    Args:
        data: Lista de listas com os dados
        pk_column_index: Índice da coluna da chave primária

    Returns:
        Set contendo IDs válidos
    """
    valid_ids = set()
    for row in data:
        if row:  # garante que a linha não esteja vazia
            pk_int = _safe_convert_to_int(row[pk_column_index])
            if pk_int:  # adiciona apenas se for um int válido e > 0
                valid_ids.add(pk_int)
    return valid_ids


def clean_foreign_key(fk_string: str, valid_pk_set: Set[int], default: int = 0) -> Optional[int]:
    """
    Recebe uma chave estrangeira (string) e o 'set' de PKs válidas.
    Retorna um int válido ou None se a FK for inválida, 0 ou nula.

    Args:
        fk_string: String com o valor da FK
        valid_pk_set: Set contendo PKs válidas

    Returns:
        Int válido ou None
    """
    fk_int = _safe_convert_to_int(fk_string)

    if fk_int is None or fk_int == 0:
        return 0

    if fk_int not in valid_pk_set:
        return 0 

    return fk_int  


# ======================================================================
# FUNÇÕES AUXILIARES (CONVERSÃO DE VALORES)
# ======================================================================

def _safe_convert_to_int(value_str: str) -> Optional[int]:
    """
    Tenta converter uma string (ex: '553,465' ou '4.0') para int.
    Remove vírgulas antes de converter. Retorna None em caso de falha.

    Args:
        value_str: String a ser convertida

    Returns:
        Int ou None em caso de falha
    """
    if not value_str:  # checa se a string é None ou vazia
        return None
    try:
        cleaned_str = value_str.replace(',', '')  # limpeza de vírgulas

        # usando float() primeiro para '4.0', depois int()
        return int(float(cleaned_str))
    except (ValueError, TypeError, OverflowError):
        # falha se for 'abc', None, ou um número muito grande
        return None


def _safe_convert_to_float(value_str: str) -> Optional[float]:
    """
    Tenta converter uma string (ex: '1,234.50') para float.
    Remove vírgulas antes de converter. Retorna None em caso de falha.

    Args:
        value_str: String a ser convertida

    Returns:
        Float ou None em caso de falha
    """
    if not value_str:  # checa se é none ou vazia
        return None
    try:
        cleaned_str = value_str.replace(',', '')

        return float(cleaned_str)
    except (ValueError, TypeError):
        return None

def _tratamento_fk_com_default(fk_string: str, valid_pk_set: Set[int], valor_default: int = 0) -> int:
    """
    Valida FK e retorna valor padrão se inválida.
    
    Args:
        fk_string: String com o valor da FK
        valid_pk_set: Set contendo PKs válidas
        valor_default: Valor padrão se FK for inválida (padrão: 0)
        
    Returns:
        Int válido ou valor_default
    """
    fk_int = _safe_convert_to_int(fk_string)
    
    if fk_int is None or fk_int == 0:
        return valor_default
    
    if fk_int not in valid_pk_set:
        return valor_default  
        
    return fk_int


# ======================================================================
# FUNÇÕES BANCO DE DADOS
# ======================================================================

def cria_banco_de_dados(nome_banco: str) -> bool:
    """
    Cria um banco de dados SQLite.
    Se o arquivo já existir, apenas abre a conexão.

    Args:
        nome_banco: Nome do arquivo do banco de dados

    Returns:
        True se bem-sucedido, False caso contrário
    """
    try:
        if not nome_banco.endswith('.db'):
            nome_banco += '.db'

        with sqlite3.connect(nome_banco) as conn:
            cursor = conn.cursor()
            print(f"Banco de dados '{nome_banco}' criado/conectado com sucesso!")
            print(f"  Versão do SQLite: {sqlite3.sqlite_version}")

        return True

    except sqlite3.Error as e:
        print(f"Erro ao criar banco de dados: {e}")
        return False


def cria_tabela_dict(colunas_tipo: dict, nome_tabela: str, nome_banco: str) -> bool:
    """
    Cria uma tabela SQLite usando dicionário com colunas e tipos.

    Args:
        colunas_tipo: Dicionário com {nome_coluna: tipo_dado}
        nome_tabela: Nome da tabela a ser criada
        nome_banco: Nome do arquivo do banco de dados

    Returns:
        True se bem-sucedido, False caso contrário
    """
    try:
        with sqlite3.connect(nome_banco) as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {nome_tabela}")

            # Constrói as definições das colunas
            colunas_definicoes = []
            for coluna, tipo in colunas_tipo.items():
                colunas_definicoes.append(f"{coluna} {tipo}")

            colunas_sql = ", ".join(colunas_definicoes)
            sql_create = f"CREATE TABLE {nome_tabela} ({colunas_sql})"
            cursor.execute(sql_create)

            conn.commit()
            print(f"Tabela '{nome_tabela}' criada com sucesso!")
            return True

    except sqlite3.Error as e:
        print(f"Erro ao criar tabela '{nome_tabela}': {e}")
        return False


def cria_pk_composta(nome_tabela: str, colunas_pk: list, nome_banco: str) -> bool:
    """
    Cria uma chave primária composta em uma tabela existente.
    Usa índice UNIQUE (equivalente a PK composta no SQLite).

    Args:
        nome_tabela: Nome da tabela
        colunas_pk: Lista de nomes de colunas que formarão a PK composta
        nome_banco: Nome do arquivo do banco de dados

    Returns:
        True se bem-sucedido, False caso contrário
    """
    try:
        if not colunas_pk or len(colunas_pk) < 2:
            print("Erro: Forneça pelo menos 2 colunas para PK composta")
            return False

        with sqlite3.connect(nome_banco) as conn:
            cursor = conn.cursor()

            nome_indice = f"pk_{nome_tabela.lower()}"
            colunas_sql = ", ".join(colunas_pk)

            sql_create_index = f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {nome_indice} 
            ON {nome_tabela}({colunas_sql})
            """

            cursor.execute(sql_create_index)
            conn.commit()

            print(f"Chave primária composta criada em '{nome_tabela}'")
            print(f"  Colunas: {colunas_sql}")
            print(f"  Índice: {nome_indice}")
            return True

    except sqlite3.Error as e:
        print(f"Erro ao criar PK composta: {e}")
        return False


def insere_dados_tabela(nome_tabela: str, colunas: list, dados: list, nome_banco: str) -> bool:
    """
    Insere dados em uma tabela do banco de dados.

    Args:
        nome_tabela (str): Nome da tabela onde os dados serão inseridos
        colunas (list): Lista com os nomes das colunas
        dados (list): Lista de tuplas com os dados a serem inseridos
        nome_banco (str): Nome do arquivo do banco de dados

    Returns:
        True se bem-sucedido, False caso contrário
    """
    try:
        with sqlite3.connect(nome_banco) as conn:
            cursor = conn.cursor()

            placeholders = ", ".join("?" for _ in colunas)
            sql_insert = f"INSERT INTO {nome_tabela} ({', '.join(colunas)}) VALUES ({placeholders})"

            cursor.executemany(sql_insert, dados)
            conn.commit()

            print(f"Dados inseridos com sucesso na tabela '{nome_tabela}'")
            return True

    except sqlite3.Error as e:
        print(f"Erro ao inserir dados na tabela '{nome_tabela}': {e}")
        return False
    


# ======================================================================
# FUNÇÃO PRINCIPAL - ORQUESTRA TODA A EXTRAÇÃO E TRANSFORMAÇÃO
# ======================================================================

def main_etl(nome_banco: str) -> bool:
    """
    Função principal que orquestra todo o processo de Extração e Transformação.

    Args:
        nome_banco: Nome do arquivo do banco de dados

    Returns:
        True se bem-sucedido, False caso contrário
    """
    print("=" * 70)
    print("Iniciando processo de ETL (Extração e Transformação) com Python puro...")
    print("=" * 70)

    # Mapeamento de Nomes de Arquivos
    file_repres = "DadosERPRepres.csv"
    file_fornclien = "DadosERPFornClien.csv"
    file_produtos = "DadosERPProdutos.csv"
    file_pedidos = "DadosERPPedidos.csv"
    file_pedidositem = "DadosERPPedidosItem.csv"

    print("\n[1/5] Lendo arquivo REPRES...")
    _, data_repres = read_csv_file(file_repres)
    if data_repres is None:
        print("Encerrando script. Verifique o nome do arquivo ou o caminho.")
        return False
    valid_repres_ids = get_valid_pk_set(data_repres, 0)  # 0: CODREPRES
    print(f"  {len(valid_repres_ids)} representantes encontrados")

    print("\n[2/5] Lendo arquivo FORNCLIEN...")
    _, data_fornclien = read_csv_file(file_fornclien)
    if data_fornclien is None:
        print("Encerrando script. Verifique o nome do arquivo ou o caminho.")
        return False
    valid_fornclien_ids = get_valid_pk_set(data_fornclien, 0)  # 0: CODCLIFOR
    print(f"  {len(valid_fornclien_ids)} fornecedores/clientes encontrados")

    print("\n[3/5] Lendo arquivo PRODUTOS...")
    _, data_produtos = read_csv_file(file_produtos)
    if data_produtos is None:
        print("Encerrando script. Verifique o nome do arquivo ou o caminho.")
        return False
    valid_produtos_ids = get_valid_pk_set(data_produtos, 0)  # 0: CODPROD
    print(f"  {len(valid_produtos_ids)} produtos encontrados")

    print("\n[4/5] Lendo arquivo PEDIDOS...")
    _, data_pedidos = read_csv_file(file_pedidos)
    if data_pedidos is None:
        print("Encerrando script. Verifique o nome do arquivo ou o caminho.")
        return False
    valid_pedidos_ids = get_valid_pk_set(data_pedidos, 0)  # 0: NUMPED
    print(f"  {len(valid_pedidos_ids)} pedidos encontrados")

    print("\n[5/5] Lendo arquivo PEDIDOSITEM...")
    _, data_pedidositem = read_csv_file(file_pedidositem)
    if data_pedidositem is None:
        print("Encerrando script. Verifique o nome do arquivo ou o caminho.")
        return False
    print(f"  {len(data_pedidositem)} itens de pedidos encontrados")

    # ====================================================================
    # TRANSFORMAÇÃO DOS DADOS
    # ====================================================================
    print("\n" + "=" * 70)
    print("TRANSFORMANDO DADOS...")
    print("=" * 70)

    print("\nTransformando REPRES...")
    cleaned_repres = []
    if data_repres:
        for row in data_repres:
            cleaned_row = (
                _safe_convert_to_int(row[0]),    # CODREPRES (PK)
                row[1] or None,                  # TIPOPESS
                row[2] or None,                  # NOMEFAN
                _safe_convert_to_float(row[3])   # COMISSAOBASE
            )
            cleaned_repres.append(cleaned_row)
    print(f"  {len(cleaned_repres)} registros transformados")

    print("\nTransformando FORNCLIEN...")
    cleaned_fornclien = []
    if data_fornclien:
        for row in data_fornclien:
            cleaned_row = (
                _safe_convert_to_int(row[0]),    # CODCLIFOR (PK)
                _safe_convert_to_int(row[1]),    # TIPOCF
                clean_foreign_key(row[2], valid_repres_ids),  # CODREPRES (FK)
                row[3] or "Não Informado",       # NOMEFAN
                row[4] or "Não Informado",       # CIDADE
                row[5] or "ND",                  # UF
                _safe_convert_to_float(row[6]) or "Não Informado",  # CODMUNICIPIO
                _safe_convert_to_int(row[7]),    # TIPOPESSOA
                _safe_convert_to_int(row[8]),    # COBRBANC
                _safe_convert_to_float(row[9]) or "Não Informado"   # PRAZOPGTO
            )
            cleaned_fornclien.append(cleaned_row)
    print(f"  {len(cleaned_fornclien)} registros transformados")

    print("\nTransformando PRODUTOS...")
    cleaned_produtos = []
    if data_produtos:
        for row in data_produtos:
            cleaned_row = (
                _safe_convert_to_int(row[0]),    # CODPROD (PK)
                row[1] or None,                  # NOMEPROD
                clean_foreign_key(row[2], valid_fornclien_ids),  # CODFORNE (FK)
                _safe_convert_to_int(row[3]),    # UNIDADE
                _safe_convert_to_float(row[4]) or "Não Informado",  # ALIQICMS
                _safe_convert_to_float(row[5]) or "Não Informado",  # VALCUSTO
                _safe_convert_to_float(row[6]) or "Não Informado" ,  # VALVENDA
                _safe_convert_to_float(row[7]) or "Não Informado" ,  # QTDEMIN
                _safe_convert_to_float(row[8]) or "Não Informado" ,  # QTDEESTQ
                _safe_convert_to_int(row[9]),    # GRUPO
                row[10] or "0",                  # CLASSESTQ
                _safe_convert_to_float(row[11]) or "Não Informado" , # COMISSAO
                _safe_convert_to_float(row[12]) or "Não Informado"  # PESOBRUTO
            )
            cleaned_produtos.append(cleaned_row)
    print(f"  {len(cleaned_produtos)} registros transformados")

    print("\nTransformando PEDIDOS...")
    cleaned_pedidos = []
    if data_pedidos:
        for row in data_pedidos:
            cleaned_row = (
                _safe_convert_to_int(row[0]) or 0,                          # NUMPED (PK)
                row[1] or None,                                             # DATAPPED
                row[2] or None,                                             # HORAPPED
                clean_foreign_key(row[3], valid_fornclien_ids) or 0,        # CODCLIEN (FK)
                row[4] or None,                                             # ES
                _safe_convert_to_int(row[5]) if _safe_convert_to_int(row[5]) in [1, 2] else 1,  # FINALIDNFE (DEFAULT 1) 
                _safe_convert_to_int(row[6]) or 2,                          # SITUACAO (DEFAULT 2)
                _safe_convert_to_float(row[7]) or 0.0,                      # PESO
                _safe_convert_to_int(row[8]) or 0,                          # PRAZOPGTO (DEFAULT 0) 
                _safe_convert_to_float(row[9]) or 0.0,                      # VALORPRODS
                _safe_convert_to_float(row[10]) or 0.0,                     # VALORDESC (DEFAULT 0)
                _safe_convert_to_float(row[11]) or 0.0,                     # VALOR
                _safe_convert_to_float(row[12]) or 0.0,                     # VALBASEICMS (DEFAULT 0)
                _safe_convert_to_float(row[13]) or 0.0,                     # VALICMS (DEFAULT 0)
                _safe_convert_to_float(row[14]) or 0.0                      # COMISSAO (DEFAULT 0)
            )

            cleaned_pedidos.append(cleaned_row)
    print(f"  {len(cleaned_pedidos)} registros transformados")

    print("\nTransformando PEDIDOSITEM...")
    cleaned_pedidositem = []
    for row in data_pedidositem:
        cleaned_row = (
            clean_foreign_key(row[0], valid_pedidos_ids),  # NUMPED (FK)
            _safe_convert_to_int(row[1]),      # NUMITEM (PK parte 2)
            clean_foreign_key(row[2], valid_produtos_ids),  # CODPROD (FK)
            _safe_convert_to_float(row[3]),    # QTDE
            _safe_convert_to_float(row[4]),    # VALUNIT
            row[5] or "UN",                    # UNID
            _safe_convert_to_float(row[6]),    # ALIQICMS
            _safe_convert_to_float(row[7]),    # COMISSAO
            _safe_convert_to_int(row[8]),      # STICMS
            _safe_convert_to_float(row[9]) or "5102",    # CFOP
            _safe_convert_to_float(row[10])    # REDUCBASEICMS
        )
        cleaned_pedidositem.append(cleaned_row)
    print(f"  {len(cleaned_pedidositem)} registros transformados")


    # ====================================================================
    # CARGA (LOAD) - Inserir dados no banco
    # ====================================================================
    print("\n" + "=" * 70)
    print("CARREGANDO DADOS NO BANCO DE DADOS")
    print("=" * 70)

    print("\n[6/6] Inserindo dados...")
    
    # Inserir REPRES
    if not insere_dados_tabela(
        "Repres",
        ["CODREPRES", "TIPOPESS", "NOMEFAN", "COMISSAOBASE"],
        cleaned_repres,
        nome_banco
    ):
        return False

    # Inserir FORNCLIEN
    if not insere_dados_tabela(
        "FornClien",
        ["CODCLIFOR", "TIPOCF", "CODREPRES", "NOMEFAN", "CIDADE", "UF", 
         "CODMUNICIPIO", "TIPOPESSOA", "COBRBANC", "PRAZOPGTO"],
        cleaned_fornclien,
        nome_banco
    ):
        return False

    # Inserir PRODUTOS
    if not insere_dados_tabela(
        "Produtos",
        ["CODPROD", "NOMEPROD", "CODFORNE", "UNIDADE", "ALIQICMS", "VALCUSTO",
         "VALVENDA", "QTDEMIN", "QTDEESTQ", "GRUPO", "CLASSEESTQ", "COMISSAO", "PESOBRUTO"],
        cleaned_produtos,
        nome_banco
    ):
        return False

    # Inserir PEDIDOS
    if not insere_dados_tabela(
        "Pedidos",
        ["NUMPED", "DATAPPED", "HORAPPED", "CODCLIEN", "ES", "FINALIDNFE", "SITUACAO",
         "PESO", "PRAZOPGTO", "VALORPRODS", "VALORDESC", "VALOR", "VALBASEICMS", "VALICMS", "COMISSAO"],
        cleaned_pedidos,
        nome_banco
    ):
        return False

    # Inserir PEDIDOSITEM
    if not insere_dados_tabela(
        "PedidosItem",
        ["NUMPED", "NUMITEM", "CODPROD", "QTDE", "VALUNIT", "UNID", "ALIQICMS",
         "COMISSAO", "STICMS", "CFOP", "REDUCBASEICMS"],
        cleaned_pedidositem,
        nome_banco
    ):
        return False

    print("\n" + "=" * 70)
    print("ETL COMPLETO FINALIZADO COM SUCESSO")
    print("=" * 70)
    print(f"\nResumo:")
    print(f"  • Repres: {len(cleaned_repres)} registros")
    print(f"  • FornClien: {len(cleaned_fornclien)} registros")
    print(f"  • Produtos: {len(cleaned_produtos)} registros")
    print(f"  • Pedidos: {len(cleaned_pedidos)} registros")
    print(f"  • PedidosItem: {len(cleaned_pedidositem)} registros")

    print("\n" + "=" * 70)
    print("ETL CONCLUÍDO COM SUCESSO")
    print("=" * 70)

    return True


def main_database(nome_banco: str) -> bool:
    """
    Função principal que orquestra a criação do banco de dados e tabelas.

    Args:
        nome_banco (str): Nome do arquivo do banco de dados

    Returns:
        True se bem-sucedido, False caso contrário
    """
    print("\n" + "=" * 70)
    print("CRIANDO ESTRUTURA DO BANCO DE DADOS")
    print("=" * 70)

    # 1. Criar banco de dados
    print("\n[1/6] Criando banco de dados...")
    if not cria_banco_de_dados(nome_banco):
        return False

    # 2. Definir estrutura das tabelas
    print("\n[2/6] Definindo tabela REPRES...")
    colunas_repres = {
        "CODREPRES": "INTEGER PRIMARY KEY",
        "TIPOPESS": "TEXT(2) NOT NULL CHECK(LENGTH(TIPOPESS) <= 2)",
        "NOMEFAN": "TEXT(20) NOT NULL CHECK(LENGTH(NOMEFAN) <= 20)",
        "COMISSAOBASE": "REAL NOT NULL DEFAULT 3"
    }
    if not cria_tabela_dict(colunas_repres, "Repres", nome_banco):
        return False

    print("\n[3/6] Definindo tabela FORNCLIEN...")
    colunas_fornclien = {
        "CODCLIFOR": "INTEGER PRIMARY KEY",
        "TIPOCF": "INTEGER NOT NULL CHECK(TIPOCF IN (1, 2))",
        "CODREPRES": "INTEGER NOT NULL DEFAULT 0 REFERENCES Repres(CODREPRES)",
        "NOMEFAN": "TEXT(50) NOT NULL CHECK(LENGTH(NOMEFAN) <= 50)",
        "CIDADE": "TEXT(50) NOT NULL",
        "UF": "TEXT(2) NOT NULL CHECK(LENGTH(UF) <= 2)",
        "CODMUNICIPIO": "INTEGER NOT NULL",
        "TIPOPESSOA": "INTEGER NOT NULL CHECK(TIPOPESSOA IN (1, 2))",
        "COBRBANC": "INTEGER NOT NULL DEFAULT 0 CHECK(COBRBANC IN (-1, 0, 1))",
        "PRAZOPGTO": "INTEGER NOT NULL DEFAULT 0"
    }
    if not cria_tabela_dict(colunas_fornclien, "FornClien", nome_banco):
        return False

    print("\n[4/6] Definindo tabela PRODUTOS...")
    colunas_produtos = {
        "CODPROD": "INTEGER PRIMARY KEY",
        "NOMEPROD": "TEXT NOT NULL CHECK(LENGTH(NOMEPROD) <= 50)",
        "CODFORNE": "INTEGER NOT NULL REFERENCES FornClien(CODCLIFOR)",
        "UNIDADE": "INTEGER NOT NULL",
        "ALIQICMS": "REAL NOT NULL DEFAULT 0",
        "VALCUSTO": "REAL NOT NULL DEFAULT 0",
        "VALVENDA": "REAL NOT NULL DEFAULT 0",
        "QTDEMIN": "REAL NOT NULL DEFAULT 1",
        "QTDEESTQ": "REAL NOT NULL",
        "GRUPO": "INTEGER NOT NULL DEFAULT 1",
        "CLASSEESTQ": "TEXT NOT NULL DEFAULT 'A' CHECK(LENGTH(CLASSEESTQ) <= 1)",
        "COMISSAO": "REAL NOT NULL DEFAULT 0",
        "PESOBRUTO": "REAL NOT NULL"
    }
    if not cria_tabela_dict(colunas_produtos, "Produtos", nome_banco):
        return False

    print("\n[5/6] Definindo tabela PEDIDOS...")
    colunas_pedidos = {
        "NUMPED": "INTEGER PRIMARY KEY",
        "DATAPPED": "TEXT(10) NOT NULL CHECK(LENGTH(DATAPPED) <= 10)",
        "HORAPPED": "TEXT(8) NOT NULL CHECK(LENGTH(HORAPPED) <= 8)",
        "CODCLIEN": "INTEGER NOT NULL REFERENCES FornClien(CODCLIFOR)",
        "ES": "TEXT(1) NOT NULL CHECK(ES IN ('S', 'E')) DEFAULT 'S'",
        "FINALIDNFE": "INTEGER NOT NULL DEFAULT 1 CHECK(FINALIDNFE IN (1, 2))",
        "SITUACAO": "INTEGER NOT NULL DEFAULT 2",
        "PESO": "REAL NOT NULL",
        "PRAZOPGTO": "INTEGER NOT NULL DEFAULT 0",
        "VALORPRODS": "REAL NOT NULL",
        "VALORDESC": "REAL NOT NULL DEFAULT 0",
        "VALOR": "REAL NOT NULL",
        "VALBASEICMS": "REAL NOT NULL DEFAULT 0",
        "VALICMS": "REAL NOT NULL DEFAULT 0",
        "COMISSAO": "REAL NOT NULL DEFAULT 0"
    }
    if not cria_tabela_dict(colunas_pedidos, "Pedidos", nome_banco):
        return False

    print("\n[6/6] Definindo tabela PEDIDOSITEM...")
    colunas_pedidositem = {
        "NUMPED": "INTEGER NOT NULL REFERENCES Pedidos(NUMPED)",
        "NUMITEM": "INTEGER NOT NULL",
        "CODPROD": "INTEGER NOT NULL REFERENCES Produtos(CODPROD)",
        "QTDE": "REAL NOT NULL",
        "VALUNIT": "REAL NOT NULL",
        "UNID": "TEXT(2) NOT NULL CHECK(LENGTH(UNID) <= 2)",
        "ALIQICMS": "REAL NOT NULL DEFAULT 0",
        "COMISSAO": "REAL NOT NULL DEFAULT 0",
        "STICMS": "INTEGER NOT NULL",
        "CFOP": "INTEGER NOT NULL DEFAULT 5102",
        "REDUCBASEICMS": "REAL NOT NULL DEFAULT 0"
    }
    if not cria_tabela_dict(colunas_pedidositem, "PedidosItem", nome_banco):
        return False

    # 7. Criar chave primária composta
    print("\nCriando chave primária composta em PEDIDOSITEM...")
    if not cria_pk_composta("PedidosItem", ["NUMPED", "NUMITEM"], nome_banco):
        return False

    print("\n" + "=" * 70)
    print("BANCO DE DADOS CRIADO COM SUCESSO")
    print("=" * 70)

    return True




if __name__ == "__main__":
    nome_banco = "bd_erp_n2.db"
    
    # Criar banco de dados
    main_database(nome_banco)

    # Executar ETL
    main_etl(nome_banco)

    print("\n" + "=" * 70)
    print("PROCESSO COMPLETO FINALIZADO")
    print("=" * 70)
