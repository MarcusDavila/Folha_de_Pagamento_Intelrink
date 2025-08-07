CREATE TEMP TABLE Reduzido_Codigo AS
SELECT 
    public_folha_pagamento.cpf,
    public_folha_pagamento.tipo_pagamento,
    CASE 
        WHEN public_folha_pagamento.tipo_pagamento = '1' THEN
            CASE 
                WHEN cadastro_vinculo.cnpjcpfcodigo IS NOT NULL THEN 905
                ELSE 283
            END
        WHEN public_folha_pagamento.tipo_pagamento = '2' THEN
            CASE 
                WHEN cadastro_vinculo.cnpjcpfcodigo IS NOT NULL THEN 904
                ELSE 321
            END
        WHEN public_folha_pagamento.tipo_pagamento = '3' THEN
            CASE 
                WHEN cadastro_vinculo.cnpjcpfcodigo IS NOT NULL THEN 906
                ELSE 1056
            END
        WHEN public_folha_pagamento.tipo_pagamento = '4' THEN
            CASE 
                WHEN cadastro_vinculo.cnpjcpfcodigo IS NOT NULL THEN 908
                ELSE 494
            END
        WHEN public_folha_pagamento.tipo_pagamento = '5' THEN
            CASE 
                WHEN cadastro_vinculo.cnpjcpfcodigo IS NOT NULL THEN 956
                ELSE 955
            END
        WHEN public_folha_pagamento.tipo_pagamento = '6' THEN 957
        WHEN public_folha_pagamento.tipo_pagamento = '7' THEN
            CASE 
                WHEN public_folha_pagamento.cpf IN ('80502237015', '02824237000112', '11156023068') THEN 285
                ELSE 905
            END
        ELSE NULL 
    END AS codigo_reduzido
FROM public_folha_pagamento
LEFT JOIN cadastro_vinculo 
    ON cadastro_vinculo.cnpjcpfcodigo = public_folha_pagamento.cpf
   AND cadastro_vinculo.vinculo = '3';


-- Tabela Temporária 2: Gerando a sequência para cada linha da folha
-- (Foi corrigido um erro de sintaxe, adicionando a vírgula que faltava)
CREATE TEMP TABLE folha_com_sequencia AS
SELECT  
    public_folha_pagamento.*,
    seq.sequencia::integer as nova_sequencia
from public_folha_pagamento
CROSS JOIN LATERAL avacorpi.fnc_buscar_sequence(
    2,              -- usuario
    'contaapagar',  -- tabela
    1,              -- grupo
    1,              -- empresa
    1,              -- filial
    1,              -- unidade
    NULL,           -- tipo documento
    NULL            -- complemento
) AS seq;
 -- inserir registros na tabela Contaapagar
INSERT INTO Contaapagar (
    numerotitulo, numeroparcela, valorpendente, valorpago, valortitulo, 
    dtprevisaopagamento, dtvencimento, cnpjcpfcodigo, reduzido, dtpagamento, 
    dtemissaotitulo, dtcartorio, dtprotesto, Sequencia, grupo, empresa, filial, 
    unidade, composicao, moeda, valortitulomoeda, valormulta, valorjuro, 
    valordesconto, valordespesacartorio, valordespesaprotesto, observacao, 
    grupodocumentoorigem, empresadocumentoorigem, filialdocumentoorigem, 
    unidadedocumentoorigem, cnpjcpfcodigodocumentoorigem, 
    diferenciadornumerodocumentoorigem, seriedocumentoorigem, 
    numerosequenciadocumentoorigem, dtemissaodocumentoorigem, dtinc, 
    codigobarra, linhadigitavel, quantidadeparcela, dtalt, semaforo, 
    tipopagamento, tipodocumentoorigem, posicaocnab, nroremessa, apropriacao, 
    bancocredor, agenciacredor, contacredor, cnpjcpfcodigocredor, vinculocredor, 
    formalancamento, tipotitulo, cnpjcpfcodigoadiantamento, proprietario, veiculo, 
    reduzidodebito, formapagamento, moedapagamentooutramoeda, 
    dtcambiopagamentooutramoeda, valorcambiopagamentooutramoeda, 
    valortitulomoedapagamentooutramoeda, informarmanualmentecontrato, 
    codigocobranca, tipo, valordespesacartoriomoeda, valormultamoeda, 
    valordespesaprotestomoeda, valorjuromoeda, valordescontomoeda, valorpagomoeda, 
    valorpendentemoeda, geracreditopiscofins, naturezabasecalculocredito, 
    indicadororigemcredito, cstpis, valorbasecalculopis, percaliquotapis, 
    valorpis, cstcofins, valorbasecalculocofins, percaliquotacofins, valorcofins, 
    codigoformalancamentointerna, codigodareceitadotributo, identificadorfgts, 
    lacreconectividadesocial, digitolagreconectividadesocial, 
    valorreceitabrutaacumulada, percentualsobrereceitabrutaacumulada, 
    codigousuario, valorrecolhimento, outrosvalores, acrescimos, numeroautenticacao, 
    protocoloautenticacao, cnpjcpfcodigosacadoravalista
)
SELECT 
    CASE 
        WHEN folha_com_sequencia.tipo_pagamento = '1' THEN 'FOLHA DE PGTO ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
        WHEN folha_com_sequencia.tipo_pagamento = '2' THEN 'FOLHA DE ADTO ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
        WHEN folha_com_sequencia.tipo_pagamento = '3' THEN 'FÉRIAS'
        WHEN folha_com_sequencia.tipo_pagamento = '4' THEN 'ADTO 13º SALÁRIO'
        WHEN folha_com_sequencia.tipo_pagamento = '5' THEN 'RESCISÃO'
        WHEN folha_com_sequencia.tipo_pagamento = '6' THEN 'PENSÃO ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
        WHEN folha_com_sequencia.tipo_pagamento = '7' THEN 
            CASE 
                WHEN folha_com_sequencia.cpf IN ('80502237015', '02824237000112', '11156023068') THEN 'PRO LABORE ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
                ELSE 'FOLHA DE PGTO ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
            END
        ELSE NULL
    END AS numerotitulo,
    1 AS numeroparcela,
    REPLACE(REPLACE(folha_com_sequencia.deposito, '.', ''), ',', '.')::numeric AS valorpendente, -- CORREÇÃO ATUALIZADA
    0 AS valorpago,
    REPLACE(REPLACE(folha_com_sequencia.deposito, '.', ''), ',', '.')::numeric AS valortitulo, -- CORREÇÃO ATUALIZADA
    folha_com_sequencia.data_insercao AS dtprevisaopagamento,
    folha_com_sequencia.data_insercao AS dtvencimento,
    CASE 
        WHEN folha_com_sequencia.cpf = '11156023068' THEN '02824237000112'
        ELSE folha_com_sequencia.cpf
    END AS cnpjcpfcodigo,
    Reduzido_Codigo.codigo_reduzido,
    NULL AS dtpagamento,
    folha_com_sequencia.data_insercao AS dtemissaotitulo,
    NULL AS dtcartorio,
    NULL AS dtprotesto,
    folha_com_sequencia.nova_sequencia AS Sequencia,
    1 AS grupo,
    1 AS empresa,
    1 AS filial,
    1 AS unidade,
    1 AS composicao,
    1 AS moeda,
    REPLACE(REPLACE(folha_com_sequencia.deposito, '.', ''), ',', '.')::numeric AS valortitulomoeda, -- CORREÇÃO ATUALIZADA
    0 AS valormulta,
    0 AS valorjuro,
    0 AS valordesconto,
    0 AS valordespesacartorio,
    0 AS valordespesaprotesto,
    NULL AS observacao,
    NULL AS grupodocumentoorigem,
    NULL AS empresadocumentoorigem,
    NULL AS filialdocumentoorigem,
    NULL AS unidadedocumentoorigem,
    NULL AS cnpjcpfcodigodocumentoorigem,
    NULL AS diferenciadornumerodocumentoorigem,
    NULL AS seriedocumentoorigem,
    NULL AS numerosequenciadocumentoorigem,
    NULL AS dtemissaodocumentoorigem,
    folha_com_sequencia.data_insercao AS dtinc,
    NULL AS codigobarra,
    NULL AS linhadigitavel,
    1 AS quantidadeparcela,
    folha_com_sequencia.data_insercao AS dtalt,
    0 AS semaforo,
    NULL AS tipopagamento,
    0 AS tipodocumentoorigem,
    2 AS posicaocnab,
    NULL AS nroremessa,
    2 AS apropriacao,
    NULL AS bancocredor,
    NULL AS agenciacredor,
    NULL AS contacredor,
    NULL AS cnpjcpfcodigocredor,
    NULL AS vinculocredor,
    NULL AS formalancamento,
    4 AS tipotitulo,
    NULL AS cnpjcpfcodigoadiantamento,
    NULL AS proprietario,
    NULL AS veiculo,
    NULL AS reduzidodebito,
    2 AS formapagamento,
    NULL AS moedapagamentooutramoeda,
    NULL AS dtcambiopagamentooutramoeda,
    0 AS valorcambiopagamentooutramoeda,
    0 AS valortitulomoedapagamentooutramoeda,
    2 AS informarmanualmentecontrato,
    NULL AS codigocobranca,
    2 AS tipo,
    0 AS valordespesacartoriomoeda,
    0 AS valormultamoeda,
    0 AS valordespesaprotestomoeda,
    0 AS valorjuromoeda,
    0 AS valordescontomoeda,
    0 AS valorpagomoeda,
    REPLACE(REPLACE(folha_com_sequencia.deposito, '.', ''), ',', '.')::numeric AS valorpendentemoeda, -- CORREÇÃO ATUALIZADA
    2 AS geracreditopiscofins,
    NULL AS naturezabasecalculocredito,
    NULL AS indicadororigemcredito,
    NULL AS cstpis,
    0 AS valorbasecalculopis,
    0 AS percaliquotapis,
    0 AS valorpis,
    NULL AS cstcofins,
    0 AS valorbasecalculocofins,
    0 AS percaliquotacofins,
    0 AS valorcofins,
    1 AS codigoformalancamentointerna,
    NULL AS codigodareceitadotributo,
    NULL AS identificadorfgts,
    NULL AS lacreconectividadesocial,
    NULL AS digitolagreconectividadesocial,
    NULL AS valorreceitabrutaacumulada,
    0 AS percentualsobrereceitabrutaacumulada,
    {?SESSION.IdUsuario} AS codigousuario,
    0 AS valorrecolhimento,
    0 AS outrosvalores,
    0 AS acrescimos,
    NULL AS numeroautenticacao,
    NULL AS protocoloautenticacao,
    NULL AS cnpjcpfcodigosacadoravalista
FROM folha_com_sequencia
JOIN Reduzido_Codigo 
    ON folha_com_sequencia.cpf = Reduzido_Codigo.cpf
WHERE folha_com_sequencia.cpf IS NOT NULL 
  AND folha_com_sequencia.data_insercao IS NOT NULL 
  AND folha_com_sequencia.deposito IS NOT NULL;

-- Inserir registros na tabela Contaapagar_Composicao
INSERT INTO CONTAAPAGAR_COMPOSICAO (
    grupo, empresa, filial, unidade, sequencia, sequenciacomposicao, 
    tipodocumentoorigem, grupodocumentoorigem, empresadocumentoorigem, 
    filialdocumentoorigem, unidadedocumentoorigem, cnpjcpfcodigodocumentoorigem, 
    dtemissaodocumentoorigem, diferenciadornumerodocumentoorigem, seriedocumentoorigem, 
    numerosequenciadocumentoorigem, numeroparcela, dtinc, dtalt, dtvencimento, 
    dtprevisaopagamento, cnpjcpfcodigo, reduzido, valortitulo, quantidadeparcela, 
    codigobarra, linhadigitavel, usuariomark, sequenciaafretamentocalculo, 
    valorretencaopagamentopiscofinscsll, tipotitulo, formapagamento, tipo, 
    tipodocumento, historico, centrocusto, historicodescricao, avacorpi, origem, 
    tipooorigem, sequencianova, dtliberacaopagamento, tipoprogramacaoembarque, 
    dtliberacaovalidacao, usuarioliberacaovalidacao, valornegociadodiferenca, 
    adiantamentoliberadoautomatico, usuarioliberacaooperacional, usuarioliberacaoadiantamento, 
    dtliberacaoadiantamento, dtliberacaooperacional, usuariomarkliberacaosaldo, 
    idparcela, saldodevedor, valorsaldoreparcelamento, veiculo, proprietario, 
    valortitulopendente, valortituloutilizado, tipodocumentoacertoviagem, 
    filialacertoviagem, unidadeacertoviagem, diferenciadornumeroacertoviagem, 
    numeroacertoviagem, sequenciaacertoviagem, empresaacertoviagem
)
SELECT 
    1 AS grupo,
    1 AS empresa,
    1 AS filial,
    1 AS unidade,
    folha_com_sequencia.nova_sequencia AS sequencia,
    1 AS sequenciacomposicao,
    1 AS tipodocumentoorigem,
    1 AS grupodocumentoorigem,
    1 AS empresadocumentoorigem,
    1 AS filialdocumentoorigem,
    1 AS unidadedocumentoorigem,
    CASE 
        WHEN folha_com_sequencia.cpf = '11156023068' THEN '02824237000112'
        ELSE folha_com_sequencia.cpf
    END AS cnpjcpfcodigodocumentoorigem,
    folha_com_sequencia.data_insercao AS dtemissaodocumentoorigem,
    NULL AS diferenciadornumerodocumentoorigem,
    NULL AS seriedocumentoorigem,
    (SELECT COALESCE(MAX(numerosequenciadocumentoorigem), 0) + 1 
     FROM contaapagar_composicao 
     WHERE tipodocumentoorigem = 1 
       AND grupodocumentoorigem = 1 
       AND empresadocumentoorigem = 1 
       AND filialdocumentoorigem = 1 
       AND unidadedocumentoorigem = 1) AS numerosequenciadocumentoorigem,
    1 AS numeroparcela,
    folha_com_sequencia.data_insercao AS dtinc,
    NULL AS dtalt,
    folha_com_sequencia.data_insercao AS dtvencimento,
    folha_com_sequencia.data_insercao AS dtprevisaopagamento,
    folha_com_sequencia.cpf AS cnpjcpfcodigo,
    Reduzido_Codigo.codigo_reduzido as reduzido, 
    REPLACE(REPLACE(folha_com_sequencia.deposito, '.', ''), ',', '.')::numeric AS valortitulo, -- CORREÇÃO ATUALIZADA
    1 AS quantidadeparcela,
    NULL AS codigobarra,
    NULL AS linhadigitavel,
    NULL AS usuariomark,
    NULL AS sequenciaafretamentocalculo,
    0 AS valorretencaopagamentopiscofinscsll,
    4 AS tipotitulo,
    1 AS formapagamento,
    2 AS tipo,
    NULL AS tipodocumento,
    NULL AS historico,
    NULL AS centrocusto,
    NULL AS historicodescricao,
    2 AS avacorpi,
    NULL AS origem,
    NULL AS tipooorigem,
    NULL AS sequencianova,
    NULL AS dtliberacaopagamento,
    NULL AS tipoprogramacaoembarque,
    NULL AS dtliberacaovalidacao,
    NULL AS usuarioliberacaovalidacao,
    NULL AS valornegociadodiferenca,
    1 AS adiantamentoliberadoautomatico,
    NULL AS usuarioliberacaooperacional,
    NULL AS usuarioliberacaoadiantamento,
    NULL AS dtliberacaoadiantamento,
    NULL AS dtliberacaooperacional,
    NULL AS usuariomarkliberacaosaldo,
    NULL AS idparcela,
    NULL AS saldodevedor,
    NULL AS valorsaldoreparcelamento,
    NULL AS veiculo,
    NULL AS proprietario,
    0 AS valortitulopendente,
    0 AS valortituloutilizado,
    NULL AS tipodocumentoacertoviagem,
    NULL AS filialacertoviagem,
    NULL AS unidadeacertoviagem,
    NULL AS diferenciadornumeroacertoviagem,
    NULL AS numeroacertoviagem,
    NULL AS sequenciaacertoviagem,
    NULL AS empresaacertoviagem
FROM folha_com_sequencia
JOIN Reduzido_Codigo 
    ON folha_com_sequencia.cpf = Reduzido_Codigo.cpf
WHERE folha_com_sequencia.cpf IS NOT NULL 
  AND folha_com_sequencia.data_insercao IS NOT NULL 
  AND folha_com_sequencia.deposito IS NOT NULL;

--codigo para deletar todos os registros da tabela folha_pagamento apos a inserção na tabela Contaapagar e Contaapagar_Composicao
DELETE FROM public_folha_pagamento;