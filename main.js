// Requisitos: Instale com npm install dotenv pg exceljs fs path

import fs from 'fs';
import path from 'path';
import * as dotenv from 'dotenv';
import { Client } from 'pg';
import ExcelJS from 'exceljs';

dotenv.config();

const PATTERN_VALOR = /^\d{1,3}(?:\.\d{3})*,\d{2}$|^\d+,\d{2}$/;
const PATTERN_CPF = /\d{3}\.?\d{3}\.?\d{3}-?\d{2}/;

const DB_CONFIG = {
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: parseInt(process.env.DB_PORT)
};

const TIPOS_PAGAMENTO = {
  SALARIO: '1',
  ADIANTAMENTO: '2',
  FERIAS: '3',
  'ADTO 13º': '4',
  RESCISAO: '5',
  PENSAO: '6',
  'PRO LABORE E ESTAGIO': '7'
};

function determinarTipoPagamento(nomeArquivo) {
  const nome = nomeArquivo.toUpperCase();
  for (const chave in TIPOS_PAGAMENTO) {
    if (nome.includes(chave)) return TIPOS_PAGAMENTO[chave];
  }
  return null;
}

function extrairValorMonetario(celulas) {
  for (let i = celulas.length - 1; i >= 0; i--) {
    const cell = celulas[i];
    if (typeof cell === 'string' && PATTERN_VALOR.test(cell.replace(/ /g, ''))) {
      return cell;
    }
  }
  return '';
}

async function processarArquivo(filePath, tipoPagamento) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(filePath);
  const sheet = workbook.getWorksheet('Sheet1');
  const dados = [];

  sheet.eachRow((row) => {
    const linha = row.values.slice(1).map(v => v ? String(v).trim() : '');
    while (linha.length < 30) linha.push('');

    if (linha.every(item => item === '')) return;

    let resultado = null;
    if (tipoPagamento === '6') resultado = processarPensao(linha);
    else if (tipoPagamento === '7') resultado = processarEspecie(linha);
    else resultado = processarDepositoConta(linha);

    if (resultado) dados.push(resultado);
  });

  return dados;
}

function processarDepositoConta(linha) {
  let ordem = linha[1], unidade = linha[2], contrato = linha[4], nome = linha[5];
  let cpf = linha.find(cell => PATTERN_CPF.test(cell)) || '';

  let banco = '', agencia = '', num_conta = '';
  for (let i = 0; i < linha.length; i++) {
    if (linha[i].includes('/')) {
      const partes = linha[i].split('/');
      if (partes.length >= 2) {
        banco = partes[0];
        agencia = partes[1];
        for (let j = i + 1; j < linha.length; j++) {
          if (!['-', '/'].includes(linha[j])) {
            num_conta = linha[j];
            break;
          }
        }
        break;
      }
    }
  }

  const deposito = extrairValorMonetario(linha);
  return [ordem, unidade, contrato, nome, cpf, banco, agencia, num_conta, deposito];
}

function processarPensao(linha) {
  let pensionista = linha[0], funcionario = linha[3], unidade = linha[5], cpf = linha[6];
  let banco = '', agencia = '', num_conta = '';
  for (let i = 8; i < linha.length; i++) {
    if (linha[i].includes('/')) {
      const partes = linha[i].split('/');
      if (partes.length >= 2) {
        banco = partes[0];
        agencia = partes[1];
        for (let j = i + 1; j < linha.length; j++) {
          if (!['-', '/'].includes(linha[j])) {
            num_conta = linha[j];
            break;
          }
        }
        break;
      }
    }
  }
  const valor = extrairValorMonetario(linha);
  return ['', unidade, '', pensionista, cpf, banco, agencia, num_conta, valor];
}

function processarEspecie(linha) {
  let ordem = linha[1], contrato = linha[2], nome = linha[3];
  let cpf = '';
  for (let i = 7; i <= 9; i++) {
    if (PATTERN_CPF.test(linha[i])) {
      cpf = linha[i];
      break;
    }
  }
  const deposito = extrairValorMonetario(linha);
  return [ordem, '', contrato, nome, cpf, '', '', '', deposito];
}

function formatarValorMonetario(valor) {
  if (!valor) return '';
  let valorLimpo = valor.replace(/[^\d,]/g, '');
  if (!valorLimpo.includes(',')) return parseInt(valorLimpo).toLocaleString('pt-BR') + ',00';
  let [inteiro, decimal] = valorLimpo.split(',');
  decimal = decimal.padEnd(2, '0').slice(0, 2);
  return parseInt(inteiro).toLocaleString('pt-BR') + ',' + decimal;
}

async function criarTabelaPostgres(client) {
  await client.query(`
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
    )
  `);
}

async function inserirDadosPostgres(client, dados, tipoPagamento) {
  await criarTabelaPostgres(client);
  for (const row of dados) {
    try {
      await client.query(
        `INSERT INTO public_folha_pagamento 
          (ordem, unidade, contrato, nome, cpf, banco, agencia, num_conta, deposito, tipo_pagamento)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [...row, tipoPagamento]
      );
    } catch (err) {
      console.error('Erro ao inserir linha:', row, err.message);
    }
  }
}

async function main() {
  const pasta = path.resolve();
  const arquivos = fs.readdirSync(pasta).filter(f => f.endsWith('.xlsx'));
  if (!arquivos.length) return console.log('Nenhum arquivo .xlsx encontrado.');

  const client = new Client(DB_CONFIG);
  await client.connect();

  let total = 0;
  for (const arquivo of arquivos) {
    const tipo = determinarTipoPagamento(arquivo);
    if (!tipo) {
      console.log(`Tipo de pagamento não identificado: ${arquivo}`);
      continue;
    }
    const caminho = path.join(pasta, arquivo);
    const dados = await processarArquivo(caminho, tipo);
    dados.forEach((r, i) => r[8] = formatarValorMonetario(r[8]));
    await inserirDadosPostgres(client, dados, tipo);
    total += dados.length;
    console.log(`Processado: ${arquivo} (${dados.length} registros)`);
  }

  await client.end();
  console.log(`Processo finalizado. Total de registros: ${total}`);
}

main();