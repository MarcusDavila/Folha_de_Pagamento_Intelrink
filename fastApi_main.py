import os
import re
import pandas as pd
import psycopg2
from psycopg2 import sql
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime

# Variaveis para identificar valores monetários e CPFs
PATTERN_VALOR = re.compile(r'^\d{1,3}(?:\.\d{3})*,\d{2}$|^\d+,\d{2}$')
PATTERN_CPF   = re.compile(r'\d{3}\.?\d{3}\.?\d{3}-?\d{2}')

DB_CONFIG = {
    'dbname':   'interlink',
    'user':     '************',
    'password': '************',
    'host':     '138.2.245.181',
    'port':     '5432'
}

TIPOS_PAGAMENTO = {
    "SALARIO":               "1",
    "ADIANTAMENTO":          "2",
    "FERIAS":                "3",
    "ADTO 13º":              "4",
    "RESCISAO":              "5",
    "PENSAO":                "6",
    "PRO LABORE E ESTAGIO":  "7"
}

# --- Funções de processamento ---
def determinar_tipo_pagamento(nome_arquivo):
    nome = nome_arquivo.upper()
    for chave, codigo in TIPOS_PAGAMENTO.items():
        if chave in nome:
            return codigo
    return None

def extrair_valor_monetario(celulas):
    for cell in reversed(celulas):
        if isinstance(cell, str) and PATTERN_VALOR.match(cell.replace(' ', '')):
            return cell
    return ""

def processar_deposito_conta(linha):
    try:
        ordem   = linha[1].strip() if len(linha) > 1 else ""
        unidade = linha[2].strip() if len(linha) > 2 else ""
        contrato= linha[4].strip() if len(linha) > 4 else ""
        nome    = linha[5].strip() if len(linha) > 5 else ""

        # CPF
        cpf = ""
        for i, cel in enumerate(linha):
            if PATTERN_CPF.match(str(cel).replace(' ', '')):
                cpf = str(cel).strip()
                break

        # Banco/Agência
        banco = agencia = ""
        for i, cel in enumerate(linha):
            if isinstance(cel, str) and '/' in cel:
                partes = cel.split('/')
                if len(partes) >= 2:
                    banco, agencia = partes[0].strip(), partes[1].strip()
                    idx = i
                    break

        # Conta
        num_conta = ""
        if banco:
            for cel in linha[idx+1:]:
                if cel and str(cel).strip() not in ['-', '/']:
                    num_conta = str(cel).strip()
                    break

        deposito = extrair_valor_monetario(linha)
        return [ordem, unidade, contrato, nome, cpf, banco, agencia, num_conta, deposito]
    except Exception as e:
        print(f"Erro processar_deposito_conta: {e}")
        return None

def processar_pensao(linha):
    try:
        pensionista       = linha[0].strip() if len(linha) > 0 else ""
        funcionario       = linha[3].strip() if len(linha) > 3 else ""
        unidade_contrato  = linha[5].strip() if len(linha) > 5 else ""
        cpf               = linha[6].strip() if len(linha) > 6 else ""

        banco = agencia = ""
        for i in range(8, len(linha)):
            cel = linha[i]
            if isinstance(cel, str) and '/' in cel:
                partes = cel.split('/')
                if len(partes) >= 2:
                    banco, agencia = partes[0].strip(), partes[1].strip()
                    idx = i
                    break

        num_conta = ""
        for cel in linha[idx+1:]:
            if cel and str(cel).strip() not in ['-', '/']:
                num_conta = str(cel).strip()
                break

        valor = extrair_valor_monetario(linha)
        return ["", unidade_contrato, "", pensionista, cpf, banco, agencia, num_conta, valor]
    except Exception as e:
        print(f"Erro processar_pensao: {e}")
        return None

def processar_especie(linha):
    try:
        ordem    = linha[1].strip() if len(linha) > 1 else ""
        contrato = linha[2].strip() if len(linha) > 2 else ""
        nome     = linha[3].strip() if len(linha) > 3 else ""

        # CPF
        cpf = ""
        for i in range(7, min(10, len(linha))):
            if PATTERN_CPF.match(str(linha[i]).replace(' ', '')):
                cpf = str(linha[i]).strip()
                break

        deposito = extrair_valor_monetario(linha)
        return [ordem, "", contrato, nome, cpf, "", "", "", deposito]
    except Exception as e:
        print(f"Erro processar_especie: {e}")
        return None

def processar_arquivo(caminho, tipo_pagamento):
    try:
        df = None
        # tenta engines
        for engine in ("openpyxl","xlrd",""):
            try:
                df = pd.read_excel(caminho, sheet_name="Sheet1",
                                   header=None,
                                   engine=engine or None)
                break
            except: pass

        if df is None:
            return None

        dados = []
        for _, row in df.iterrows():
            linha = row.tolist()
            linha += [""]*(30-len(linha))
            linha = ["" if pd.isna(x) else x for x in linha]
            linha_str = [str(x) for x in linha]
            if all(x.strip()=="" for x in linha_str):
                continue

            if tipo_pagamento=="6":
                res = processar_pensao(linha_str)
            elif tipo_pagamento=="7":
                res = processar_especie(linha_str)
            else:
                res = processar_deposito_conta(linha_str)

            if res: dados.append(res)

        if not dados:
            return None

        cols = ['ordem','unidade','contrato','nome','cpf',
                'banco','agencia','num_conta','deposito']
        return pd.DataFrame(dados, columns=cols)
    except Exception as e:
        print(f"Erro processar_arquivo ({caminho}): {e}")
        return None

def criar_tabela_postgres(conn):
    query = """
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
    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()

def inserir_dados_postgres(conn, df, tipo_pagamento):
    criar_tabela_postgres(conn)
    insert = sql.SQL("""
    INSERT INTO public_folha_pagamento
      (ordem, unidade, contrato, nome, cpf,
       banco, agencia, num_conta, deposito, tipo_pagamento)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """)
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            tpl = tuple(row) + (tipo_pagamento,)
            try:
                cur.execute(insert, tpl)
            except Exception as e:
                print(f"Erro inserir linha {row}: {e}")
                conn.rollback()
        conn.commit()

def formatar_valor_monetario(valor):
    if not valor: return ""
    v = re.sub(r'[^\d,]',"", str(valor))
    if ',' not in v:
        return f"{int(v):,}".replace(",", "X").replace(".",",").replace("X",".") + ",00"
    inteiro, dec = v.split(',',1)
    dec = (dec+"00")[:2]
    parte = f"{int(inteiro):,}".replace(",", "X").replace(".",",").replace("X",".")
    return f"{parte},{dec}"

# --- FastAPI ---
app = FastAPI(title="Importador Folha Pagamento")

@app.post("/upload_excel/")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(400, "Apenas .xlsx permitido")
    ts = int(datetime.utcnow().timestamp()*1000)
    temp = f"/tmp/{ts}_{file.filename}"
    with open(temp,"wb") as f:
        f.write(await file.read())

    tipo = determinar_tipo_pagamento(file.filename)
    if tipo is None:
        os.remove(temp)
        raise HTTPException(400,"Tipo de pagamento não identificado")

    df = processar_arquivo(temp, tipo)
    os.remove(temp)
    if df is None or df.empty:
        raise HTTPException(422,"Nenhum dado processado")

    df['deposito'] = df['deposito'].apply(formatar_valor_monetario)
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        inserir_dados_postgres(conn, df, tipo)
        conn.close()
    except Exception as e:
        raise HTTPException(500, f"Erro no banco: {e}")

    return JSONResponse({"filename": file.filename, "rows": len(df)})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
