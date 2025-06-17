import pandas as pd
import os
import re
import psycopg2
from psycopg2 import sql

# Padrão para identificar valores monetários
PATTERN_VALOR = re.compile(r'^\d{1,3}(?:\.\d{3})*,\d{2}$|^\d+,\d{2}$')
PATTERN_CPF = re.compile(r'\d{3}\.?\d{3}\.?\d{3}-?\d{2}')

# CONFIGURAÇÕES DO BANCO - INSIRA SEUS DADOS AQUI!
DB_CONFIG = {
    'dbname': 'interlink',
    'user': 'user_interlink',
    'password': '41Ha1bGT0Lhi',
    'host': '138.2.245.181',
    'port': '5432'
}

def obter_tipo_pagamento():
    print("\nEscolha o tipo de pagamento:")
    print("1 - Salário")
    print("2 - Adiantamento")
    print("3 - Férias")
    while True:
        escolha = input("Digite o número correspondente (1, 2 ou 3): ").strip()
        if escolha in ['1', '2', '3']:
            return escolha
        print("Opção inválida. Tente novamente.")
        
def extrair_valor_monetario(celulas):
    """Procura um valor monetário em uma lista de células, do fim para o início"""
    for cell in reversed(celulas):
        if PATTERN_VALOR.match(cell.replace(' ', '')):
            return cell
    return ""

def processar_folha1(linha):
    """Processa linhas no formato padrão (FOLHA1)"""
    try:
        ordem = linha[1].strip()
        unidade = linha[2].strip()
        contrato = linha[4].strip() if len(linha) > 4 else ""
        nome = linha[5].strip() if len(linha) > 5 else ""
        cpf = linha[8].strip() if len(linha) > 8 else ""
        banco = linha[11].strip() if len(linha) > 11 else ""
        agencia = linha[13].strip() if len(linha) > 13 else ""
        
        # Tratar número da conta
        num_conta = ""
        if len(linha) > 15:
            num_conta = linha[15].strip()
            if num_conta == '-' and len(linha) > 16:
                num_conta = linha[16].strip()
        
        # Extrair depósito
        deposito = ""
        if len(linha) > 17:
            deposito = linha[17].strip()
        
        if not deposito or not PATTERN_VALOR.match(deposito.replace(' ', '')):
            deposito = extrair_valor_monetario(linha)
        
        return [ordem, unidade, contrato, nome, cpf, banco, agencia, num_conta, deposito]
    
    except Exception as e:
        print(f"Erro ao processar linha FOLHA1: {str(e)}")
        return None

def processar_folha2(linha):
    """Processa linhas no formato alternativo (FOLHA2) - pagamento em espécie"""
    try:
        ordem = linha[1].strip()
        contrato = linha[2].strip() if len(linha) > 2 else ""
        nome = linha[3].strip() if len(linha) > 3 else ""
        
        cpf = ""
        for i in range(7, 10):
            if i < len(linha) and PATTERN_CPF.match(linha[i].replace(' ', '')):
                cpf = linha[i].strip()
                break
        
        deposito = ""
        if len(linha) > 10:
            deposito = linha[10].strip()
        
        if not deposito or not PATTERN_VALOR.match(deposito.replace(' ', '')):
            deposito = extrair_valor_monetario(linha)
        
        return [ordem, "", contrato, nome, cpf, "", "", "", deposito]
    
    except Exception as e:
        print(f"Erro ao processar linha FOLHA2: {str(e)}")
        return None

def processar_arquivo(caminho_entrada):
    """Processa um arquivo Excel e retorna os dados formatados"""
    try:
        nome_arquivo = os.path.basename(caminho_entrada)
        print(f"\nProcessando arquivo: {nome_arquivo}")
        
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
            linha += [''] * (20 - len(linha))
            linha = [str(item) if not pd.isna(item) else "" for item in linha]
            
            if len(linha) > 1 and linha[1].strip().isdigit():
                if "FOLHA1" in nome_arquivo.upper():
                    resultado = processar_folha1(linha)
                    if resultado is not None:
                        dados.append(resultado)
                elif "FOLHA2" in nome_arquivo.upper():
                    if any("TOTAL" in cell.upper() for cell in linha):
                        continue
                    resultado = processar_folha2(linha)
                    if resultado is not None:
                        dados.append(resultado)
                else:
                    if any(linha[i].isdigit() for i in [1, 2, 3]):
                        resultado = processar_folha1(linha)
                        if resultado is not None:
                            dados.append(resultado)
                    else:
                        resultado = processar_folha2(linha)
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
    """Cria a tabela se não existir no banco de dados"""
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

def inserir_dados_postgres(conn, df):
    """Insere os dados processados no PostgreSQL"""
    criar_tabela_postgres(conn)
    
    insert_query = sql.SQL("""
    INSERT INTO public_folha_pagamento 
        (ordem, unidade, contrato, nome, cpf, banco, agencia, num_conta, deposito, tipo_pagamento)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            data_tuple = tuple(row)
            try:
                cursor.execute(insert_query, data_tuple)
            except Exception as e:
                print(f"Erro ao inserir linha: {row}\nErro: {str(e)}")
                conn.rollback()
                continue
        conn.commit()

if __name__ == "__main__":
    pasta_script = os.path.dirname(os.path.abspath(__file__))

    # Solicitar tipo de pagamento ao usuário
    tipo_pagamento = obter_tipo_pagamento()

    arquivos_entrada = [
        os.path.join(pasta_script, "FOLHA1.xlsx"),
        os.path.join(pasta_script, "FOLHA2.xlsx")
    ]
    
    arquivos_faltantes = [f for f in arquivos_entrada if not os.path.exists(f)]
    if arquivos_faltantes:
        print("\nERRO: Arquivos não encontrados:")
        for f in arquivos_faltantes:
            print(f"- {os.path.basename(f)}")
        print("\nCertifique-se que os arquivos estão na mesma pasta do script.")
        input("\nPressione Enter para sair...")
        exit()
    
    # Processar arquivos
    dfs = []
    for arquivo in arquivos_entrada:
        df = processar_arquivo(arquivo)
        if df is not None:
            dfs.append(df)
    
    if not dfs:
        print("\nNenhum dado válido encontrado nos arquivos.")
        input("\nPressione Enter para sair...")
        exit()
    
    df_final = pd.concat(dfs, ignore_index=True)
    df_final['tipo_pagamento'] = tipo_pagamento  # Adiciona a coluna ao DataFrame
    
    # Conectar ao PostgreSQL e inserir dados
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexão com o PostgreSQL estabelecida com sucesso!")
        
        inserir_dados_postgres(conn, df_final)
        print(f"\nTotal de registros inseridos: {len(df_final)}")
        
    except Exception as e:
        print(f"\nERRO na conexão com o banco de dados: {str(e)}")
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco encerrada.")
    
    print("\nProcesso concluído! Dados inseridos no banco PostgreSQL.")
    input("\nPressione Enter para sair...")
    exit()
    
    # Processar arquivos
    dfs = []
    for arquivo in arquivos_entrada:
        df = processar_arquivo(arquivo)
        if df is not None:
            dfs.append(df)
    
    if not dfs:
        print("\nNenhum dado válido encontrado nos arquivos.")
        input("\nPressione Enter para sair...")
        exit()
    
    df_final = pd.concat(dfs, ignore_index=True)
    
    # Conectar ao PostgreSQL e inserir dados
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexão com o PostgreSQL estabelecida com sucesso!")
        
        inserir_dados_postgres(conn, df_final)
        print(f"\nTotal de registros inseridos: {len(df_final)}")
        
    except Exception as e:
        print(f"\nERRO na conexão com o banco de dados: {str(e)}")
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco encerrada.")
    
    print("\nProcesso concluído! Dados inseridos no banco PostgreSQL.")
    input("\nPressione Enter para sair...")