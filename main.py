import pandas as pd
import os
import re
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Padrões para identificar valores monetários e CPFs
PATTERN_VALOR = re.compile(r'^\d{1,3}(?:\.\d{3})*,\d{2}$|^\d+,\d{2}$')
PATTERN_CPF = re.compile(r'\d{3}\.?\d{3}\.?\d{3}-?\d{2}')

# CONFIGURAÇÕES DO BANCO - INSIRA SEUS DADOS AQUI!
DB_CONFIG = {
    'dbname': 'interlink',
    'user': '**********',
    'password': '*********',
    'host': '138.2.245.181',
    'port': '5432'
}

# Dicionário para mapear tipos de pagamento com base no nome do arquivo
TIPOS_PAGAMENTO = {
    "SALARIO": "1",
    "ADIANTAMENTO": "2",
    "FERIAS": "3",
    "ADTO 13º": "4",
    "RESCISAO": "5",
    "PENSAO": "6",
    "PRO LABORE E ESTAGIO": "7"
}

def determinar_tipo_pagamento(nome_arquivo):
    """Determina o tipo de pagamento com base no nome do arquivo."""
    nome = nome_arquivo.upper()
    for chave in TIPOS_PAGAMENTO:
        if chave in nome:
            return TIPOS_PAGAMENTO[chave]
    return None

def extrair_valor_monetario(celulas):
    """Procura um valor monetário em uma lista de células, do fim para o início."""
    for cell in reversed(celulas):
        if isinstance(cell, str) and PATTERN_VALOR.match(cell.replace(' ', '')):
            return cell
    return ""

def processar_deposito_conta(linha):
    """Processa arquivos de depósito em conta (SALARIO, ADIANTAMENTO, FERIAS, ADTO 13º, RESCISAO)"""
    try:
        # Extrair campos básicos
        ordem = linha[1].strip() if len(linha) > 1 else ""
        unidade = linha[2].strip() if len(linha) > 2 else ""
        contrato = linha[4].strip() if len(linha) > 4 else ""
        nome = linha[5].strip() if len(linha) > 5 else ""
        
        # Procurar CPF na linha
        cpf = ""
        for i in range(len(linha)):
            if PATTERN_CPF.match(str(linha[i]).replace(' ', '')):
                cpf = str(linha[i]).strip()
                break
        
        # Procurar banco/agência (padrão: número, barra, número)
        banco = ""
        agencia = ""
        for i in range(len(linha)):
            if isinstance(linha[i], str) and '/' in linha[i]:
                partes = linha[i].split('/')
                if len(partes) >= 2:
                    banco = partes[0].strip()
                    agencia = partes[1].strip()
                    break
        
        # Procurar número da conta (geralmente após o padrão banco/agência)
        num_conta = ""
        if banco:
            for i in range(i+1, len(linha)):
                if linha[i] and str(linha[i]).strip() not in ['-', '/']:
                    num_conta = str(linha[i]).strip()
                    break
        
        # Extrair valor do depósito
        deposito = extrair_valor_monetario(linha)
        
        return [ordem, unidade, contrato, nome, cpf, banco, agencia, num_conta, deposito]
    
    except Exception as e:
        print(f"Erro ao processar linha de depósito: {str(e)}")
        return None

def processar_pensao(linha):
    """Processa arquivos de pensão (PENSAO.xlsx)"""
    try:
        # Extrair campos específicos de pensão
        pensionista = linha[0].strip() if len(linha) > 0 else ""
        funcionario = linha[3].strip() if len(linha) > 3 else ""
        unidade_contrato = linha[5].strip() if len(linha) > 5 else ""
        cpf = linha[6].strip() if len(linha) > 6 else ""
        
        # Banco/Agência (padrão: número, barra, número)
        banco = ""
        agencia = ""
        for i in range(8, len(linha)):
            if isinstance(linha[i], str) and '/' in linha[i]:
                partes = linha[i].split('/')
                if len(partes) >= 2:
                    banco = partes[0].strip()
                    agencia = partes[1].strip()
                    break
        
        # Número da conta
        num_conta = ""
        for i in range(i+1, len(linha)):
            if linha[i] and str(linha[i]).strip() not in ['-', '/']:
                num_conta = str(linha[i]).strip()
                break
        
        # Valor
        valor = extrair_valor_monetario(linha)
        
        # Usar o nome do pensionista como "nome"
        return ["", unidade_contrato, "", pensionista, cpf, banco, agencia, num_conta, valor]
    
    except Exception as e:
        print(f"Erro ao processar linha de pensão: {str(e)}")
        return None

def processar_especie(linha):
    """Processa arquivos de pagamento em espécie (PRO LABORE E ESTAGIO.xlsx)"""
    try:
        ordem = linha[1].strip() if len(linha) > 1 else ""
        contrato = linha[2].strip() if len(linha) > 2 else ""
        nome = linha[3].strip() if len(linha) > 3 else ""
        
        # Procurar CPF na linha
        cpf = ""
        for i in range(7, 10):
            if i < len(linha) and PATTERN_CPF.match(str(linha[i]).replace(' ', '')):
                cpf = str(linha[i]).strip()
                break
        
        # Valor
        deposito = extrair_valor_monetario(linha)
        
        return [ordem, "", contrato, nome, cpf, "", "", "", deposito]
    
    except Exception as e:
        print(f"Erro ao processar linha de espécie: {str(e)}")
        return None

def processar_arquivo(caminho_entrada, tipo_pagamento):
    """Processa um arquivo Excel e retorna os dados formatados."""
    try:
        nome_arquivo = os.path.basename(caminho_entrada)
        print(f"\nProcessando arquivo: {nome_arquivo} - Tipo: {tipo_pagamento}")
        
        # Tentar diferentes engines para leitura
        try:
            df = pd.read_excel(caminho_entrada, sheet_name='Sheet1', header=None, engine='openpyxl')
        except:
            try:
                df = pd.read_excel(caminho_entrada, sheet_name='Sheet1', header=None, engine='xlrd')
            except:
                df = pd.read_excel(caminho_entrada, sheet_name='Sheet1', header=None)
        
        dados = []
        for index, row in df.iterrows():
            linha = row.tolist()
            linha += [''] * (30 - len(linha))  # Preencher com valores vazios
            linha = [item if not pd.isna(item) else "" for item in linha]
            
            # Converter todos os elementos para string para processamento
            linha_str = [str(item) for item in linha]
            
            # Ignorar linhas vazias ou de cabeçalho
            if all(item.strip() == "" for item in linha_str):
                continue
                
            # Selecionar o processador com base no tipo de pagamento
            resultado = None
            if tipo_pagamento == "6":  # Pensão
                resultado = processar_pensao(linha_str)
            elif tipo_pagamento == "7":  # Pro Labore e Estágio
                resultado = processar_especie(linha_str)
            else:  # Depósito em conta (1-5)
                resultado = processar_deposito_conta(linha_str)
            
            if resultado is not None:
                dados.append(resultado)
        
        if not dados:
            print(f"AVISO: Nenhum dado encontrado em {nome_arquivo}")
            return None
            
        colunas = [
            'ordem', 'unidade', 'contrato', 'nome', 'cpf',
            'banco', 'agencia', 'num_conta', 'deposito'
        ]
        return pd.DataFrame(dados, columns=colunas)

    except Exception as e:
        print(f"ERRO no processamento de {nome_arquivo}: {str(e)}")
        return None

def criar_tabela_postgres(conn):
    """Cria a tabela se não existir no banco de dados."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS public_folha_pagamento (
        id SERIAL PRIMARY KEY,
        ordem VARCHAR,
        unidade VARCHAR,
        contrato VARCHAR,
        nome VARCHAR,
        cpf VARCHAR,
        banco VARCHAR,
        agencia VARCHAR,
        num_conta VARCHAR,
        deposito VARCHAR,
        tipo_pagamento VARCHAR,
        data_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()

def inserir_dados_postgres(conn, df, tipo_pagamento):
    """Insere os dados processados no PostgreSQL."""
    criar_tabela_postgres(conn)
    
    insert_query = sql.SQL("""
    INSERT INTO public_folha_pagamento 
        (ordem, unidade, contrato, nome, cpf, banco, agencia, num_conta, deposito, tipo_pagamento)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            data_tuple = tuple(row) + (tipo_pagamento,)
            try:
                cursor.execute(insert_query, data_tuple)
            except Exception as e:
                print(f"Erro ao inserir linha: {row}\nErro: {str(e)}")
                conn.rollback()
                continue
        conn.commit()

def formatar_valor_monetario(valor):
    """Formata valores monetários para o padrão brasileiro (1.234,56)"""
    if not valor:
        return ""
    
    # Remover espaços e caracteres não numéricos
    valor_limpo = re.sub(r'[^\d,]', '', str(valor))
    
    # Se não tiver vírgula, adicionar ,00
    if ',' not in valor_limpo:
        return f"{int(valor_limpo):,}".replace(",", "X").replace(".", ",").replace("X", ".") + ",00"
    
    # Já tem parte decimal
    partes = valor_limpo.split(',')
    inteiro = partes[0]
    decimal = partes[1].ljust(2, '0')[:2]  # Garantir 2 dígitos
    
    # Formatar parte inteira com pontos de milhar
    inteiro_formatado = f"{int(inteiro):,}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{inteiro_formatado},{decimal}"

if __name__ == "__main__":
    pasta_script = os.path.dirname(os.path.abspath(__file__))
    
    # Obter lista de arquivos Excel na pasta
    arquivos_excel = [f for f in os.listdir(pasta_script) if f.endswith('.xlsx')]
    
    if not arquivos_excel:
        print("\nNenhum arquivo Excel encontrado na pasta.")
        input("\nPressione Enter para sair...")
        exit()
    
    # Conectar ao PostgreSQL
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexão com o PostgreSQL estabelecida com sucesso!")
    except Exception as e:
        print(f"\nERRO na conexão com o banco de dados: {str(e)}")
        input("\nPressione Enter para sair...")
        exit()
    
    # Processar cada arquivo Excel
    total_registros = 0
    for arquivo in arquivos_excel:
        caminho_arquivo = os.path.join(pasta_script, arquivo)
        tipo = determinar_tipo_pagamento(arquivo)
        if tipo is None:
            print(f"AVISO: Tipo de pagamento não determinado para {arquivo}. Arquivo ignorado.")
            continue
        
        df = processar_arquivo(caminho_arquivo, tipo)
        if df is None or df.empty:
            print(f"AVISO: Nenhum dado processado para {arquivo}")
            continue
        
        # Formatar valores monetários
        df['deposito'] = df['deposito'].apply(formatar_valor_monetario)
        
        try:
            inserir_dados_postgres(conn, df, tipo)
            print(f"Total de registros inseridos para {arquivo}: {len(df)}")
            total_registros += len(df)
        except Exception as e:
            print(f"ERRO ao inserir dados de {arquivo}: {str(e)}")
    
    # Fechar conexão
    if conn:
        conn.close()
        print("\nConexão com o banco encerrada.")
    
    print(f"\nProcesso concluído! Total de registros inseridos: {total_registros}")
    input("\nPressione Enter para sair...")