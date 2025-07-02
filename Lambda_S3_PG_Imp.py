import os
import re
import boto3
import pandas as pd
import psycopg2
from psycopg2 import sql
from io import BytesIO

# Compilando padrões regex
PATTERN_VALOR = re.compile(r'^\d{1,3}(?:\.\d{3})*,\d{2}$|^\d+,\d{2}$')
PATTERN_CPF = re.compile(r'\d{3}\.??\d{3}\.??\d{3}-??\d{2}')

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
    nome = nome_arquivo.upper()
    for chave in TIPOS_PAGAMENTO:
        if chave in nome:
            return TIPOS_PAGAMENTO[chave]
    return None

def extrair_valor_monetario(celulas):
    for cell in reversed(celulas):
        if isinstance(cell, str) and PATTERN_VALOR.match(cell.replace(' ', '')):
            return cell
    return ""

def processar_deposito_conta(linha):
    try:
        ordem = linha[1].strip() if len(linha) > 1 else ""
        unidade = linha[2].strip() if len(linha) > 2 else ""
        contrato = linha[4].strip() if len(linha) > 4 else ""
        nome = linha[5].strip() if len(linha) > 5 else ""

        cpf = ""
        for i in range(len(linha)):
            if PATTERN_CPF.match(str(linha[i]).replace(' ', '')):
                cpf = str(linha[i]).strip()
                break

        banco = ""
        agencia = ""
        for i in range(len(linha)):
            if isinstance(linha[i], str) and '/' in linha[i]:
                partes = linha[i].split('/')
                if len(partes) >= 2:
                    banco = partes[0].strip()
                    agencia = partes[1].strip()
                    break

        num_conta = ""
        if banco:
            for i in range(i + 1, len(linha)):
                if linha[i] and str(linha[i]).strip() not in ['-', '/']:
                    num_conta = str(linha[i]).strip()
                    break

        deposito = extrair_valor_monetario(linha)
        return [ordem, unidade, contrato, nome, cpf, banco, agencia, num_conta, deposito]

    except Exception as e:
        print(f"Erro linha deposito: {str(e)}")
        return None

def processar_pensao(linha):
    try:
        pensionista = linha[0].strip() if len(linha) > 0 else ""
        unidade_contrato = linha[5].strip() if len(linha) > 5 else ""
        cpf = linha[6].strip() if len(linha) > 6 else ""

        banco = ""
        agencia = ""
        for i in range(8, len(linha)):
            if isinstance(linha[i], str) and '/' in linha[i]:
                partes = linha[i].split('/')
                if len(partes) >= 2:
                    banco = partes[0].strip()
                    agencia = partes[1].strip()
                    break

        num_conta = ""
        for i in range(i + 1, len(linha)):
            if linha[i] and str(linha[i]).strip() not in ['-', '/']:
                num_conta = str(linha[i]).strip()
                break

        valor = extrair_valor_monetario(linha)
        return ["", unidade_contrato, "", pensionista, cpf, banco, agencia, num_conta, valor]

    except Exception as e:
        print(f"Erro linha pensao: {str(e)}")
        return None

def processar_especie(linha):
    try:
        ordem = linha[1].strip() if len(linha) > 1 else ""
        contrato = linha[2].strip() if len(linha) > 2 else ""
        nome = linha[3].strip() if len(linha) > 3 else ""

        cpf = ""
        for i in range(7, 10):
            if i < len(linha) and PATTERN_CPF.match(str(linha[i]).replace(' ', '')):
                cpf = str(linha[i]).strip()
                break

        deposito = extrair_valor_monetario(linha)
        return [ordem, "", contrato, nome, cpf, "", "", "", deposito]

    except Exception as e:
        print(f"Erro linha especie: {str(e)}")
        return None

def formatar_valor_monetario(valor):
    if not valor:
        return ""
    valor_limpo = re.sub(r'[^\d,]', '', str(valor))
    if ',' not in valor_limpo:
        return f"{int(valor_limpo):,}".replace(",", "X").replace(".", ",").replace("X", ".") + ",00"
    partes = valor_limpo.split(',')
    inteiro = partes[0]
    decimal = partes[1].ljust(2, '0')[:2]
    inteiro_formatado = f"{int(inteiro):,}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{inteiro_formatado},{decimal}"

def inserir_dados_postgres(dados, tipo_pagamento):
    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT']
    )
    cursor = conn.cursor()
    cursor.execute("""
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
    """)
    insert_query = sql.SQL("""
        INSERT INTO public_folha_pagamento 
        (ordem, unidade, contrato, nome, cpf, banco, agencia, num_conta, deposito, tipo_pagamento)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    for row in dados:
        row[8] = formatar_valor_monetario(row[8])
        cursor.execute(insert_query, tuple(row) + (tipo_pagamento,))
    conn.commit()
    conn.close()

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read()

        tipo_pagamento = determinar_tipo_pagamento(key)
        if not tipo_pagamento:
            print(f"Tipo de pagamento não identificado para {key}")
            continue

        df = pd.read_excel(BytesIO(body), sheet_name='Sheet1', header=None)
        dados = []

        for index, row in df.iterrows():
            linha = row.tolist() + [''] * (30 - len(row))
            linha = [str(item).strip() if not pd.isna(item) else "" for item in linha]
            if all(item.strip() == "" for item in linha):
                continue

            if tipo_pagamento == "6":
                processado = processar_pensao(linha)
            elif tipo_pagamento == "7":
                processado = processar_especie(linha)
            else:
                processado = processar_deposito_conta(linha)

            if processado:
                dados.append(processado)

        if dados:
            inserir_dados_postgres(dados, tipo_pagamento)
            print(f"Inseridos {len(dados)} registros para {key}")
        else:
            print(f"Nenhum dado processado para {key}")