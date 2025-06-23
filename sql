INSERT INTO contaapagar_composicao 

(
    grupo, empresa, filial, unidade, sequencia, sequenciacomposicao,
    cnpjcpfcodigo, historicodescricao, dtinc, tipotitulo, formapagamento, tipo,
    avacorpi, adiantamentoliberadoautomatico, reduzido, valortitulo, 
    quantidadeparcela, valortitulopendente, valortituloutilizado
)
SELECT 
    1 AS grupo,                                      
    1 AS empresa,                                     
    1 AS filial,                                      
    1 AS unidade,                                     
    id AS sequencia,                                  
    1 AS sequenciacomposicao,                         
    REPLACE(cpf, '.', '') AS cnpjcpfcodigo,          
    nome AS historicodescricao,                       
    data_insercao::DATE AS dtinc,                   
    4 AS tipotitulo,                                 
    2 AS formapagamento,                             
    4 AS tipo,                                       
    2 AS avacorpi,                                 
    1 AS adiantamentoliberadoautomatico,            
    -- 0 AS reduzido,                                   
    REPLACE(REPLACE(valor, '.', ''), ',', '.')::NUMERIC(15,2) AS valortitulo, 
    1 AS quantidadeparcela,                          
    0.00 AS valortitulopendente,                     
    0.00 AS valortituloutilizado                    
FROM folha_pagamento;                                 



UPDATE Contaapagar SET 


	numerotitulo = 'FOLHA DE PGTO 052025', 
	numeroparcela = 1, 
	valorpendente = 478.22, 
	valorpago = 0, 
	valortitulo = 478.22, 
	dtprevisaopagamento = '2025-06-06', 
	dtvencimento = '2025-06-06', 
	cnpjcpfcodigo = '05888812013', 
	reduzido = 283, 
	dtpagamento = NULL, 
	dtemissaotitulo = '2025-06-06', 
	dtcartorio = NULL, 
	dtprotesto = NULL, s
	Sequencia = 838446, 
	grupo = 1, 
	empresa = 1, 
	filial = 1, 
	unidade = 1, 
	composicao = 3, 
	moeda = 1, 
	valortitulomoeda = 478.22, 
	valormulta = 0, 
	valorjuro = 0, 
	valordesconto = 0, 
	valordespesacartorio = 0, 
	valordespesaprotesto = 0, 
	observacao = NULL, 
	grupodocumentoorigem = NULL, 
	empresadocumentoorigem = NULL, 
	filialdocumentoorigem = NULL, 
	unidadedocumentoorigem = NULL, 
	cnpjcpfcodigodocumentoorigem = NULL, 
	diferenciadornumerodocumentoorigem = NULL, 
	seriedocumentoorigem = NULL, 
	numerosequenciadocumentoorigem = NULL, 
	dtemissaodocumentoorigem = NULL, 
	dtinc = '2025-06-16', 
	codigobarra = NULL, 
	linhadigitavel = NULL, 
	quantidadeparcela = 1, 
	dtalt = '2025-06-18', 
	semaforo = 0, 
	tipopagamento = NULL, 
	tipodocumentoorigem = 0, 
	posicaocnab = 2, 
	nroremessa = NULL, 
	apropriacao = 2, 
	bancocredor = NULL, 
	agenciacredor = NULL, 
	contacredor = NULL, 
	cnpjcpfcodigocredor = NULL, 
	vinculocredor = NULL, 
	formalancamento = NULL, 
	tipotitulo = 4, 
	cnpjcpfcodigoadiantamento = NULL, 
	proprietario = NULL, 
	veiculo = NULL, 
	reduzidodebito = NULL, 
	formapagamento = 2, 
	moedapagamentooutramoeda = NULL, 
	dtcambiopagamentooutramoeda = NULL, 
	valorcambiopagamentooutramoeda = 0, 
	valortitulomoedapagamentooutramoeda = 0, 
	informarmanualmentecontrato = 2,
	codigocobranca = NULL, 
	tipo = 2, 
	valordespesacartoriomoeda = 0, 
	valormultamoeda = 0, 
	valordespesaprotestomoeda = 0, 
	valorjuromoeda = 0, 
	valordescontomoeda = 0, 
	valorpagomoeda = 0, 
	valorpendentemoeda = 478.22, 
	geracreditopiscofins = 2, 
	naturezabasecalculocredito = NULL, 
	indicadororigemcredito = NULL, 
	cstpis = NULL, 
	valorbasecalculopis = 0, 
	percaliquotapis = 0, 
	valorpis = 0, 
	cstcofins = NULL, 
	valorbasecalculocofins = 0, 
	percaliquotacofins = 0, 
	valorcofins = 0, 
	codigoformalancamentointerna = -1, 
	codigodareceitadotributo = NULL, 
	identificadorfgts = NULL, 
	lacreconectividadesocial = NULL, 
	digitolagreconectividadesocial = NULL, 
	valorreceitabrutaacumulada = NULL, 
	percentualsobrereceitabrutaacumulada = 0, 
	codigousuario = 483, 
	valorrecolhimento = 0, 
	outrosvalores = 0, 
	acrescimos = 0, 
	numeroautenticacao = NULL, 
	protocoloautenticacao = NULL, 
	cnpjcpfcodigosacadoravalista = NULL
 WHERE Contaapagar.grupo = 1 AND Contaapagar.empresa = 1 AND Contaapagar.filial = 1 AND Contaapagar.unidade = 1 AND Contaapagar.sequencia = 838446;



--variavel do contaapagar.numerotitulo


numerotitulo = CASE 
    WHEN tipo_pagamento = 1 THEN 'FOLHA DE PGTO ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
    WHEN tipo_pagamento = 2 THEN 'FOLHA DE ADTO ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
    WHEN tipo_pagamento = 3 THEN 'FÉRIAS'
    WHEN tipo_pagamento = 4 THEN 'ADTO 13º SALÁRIO'
    WHEN tipo_pagamento = 5 THEN 'RESCISÃO'
    WHEN tipo_pagamento = 6 THEN 'PENSÃO ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
    WHEN tipo_pagamento = 7 THEN 
        CASE 
            WHEN cpf = '80502237015' THEN 'PRO LABORE ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
            ELSE 'FOLHA DE PGTO ' || TO_CHAR(CURRENT_DATE, 'MMYYYY')
        END
    ELSE NULL
END


TIPOS_PAGAMENTO = {
    "SALARIO": "1",
    "ADIANTAMENTO": "2",
    "FERIAS": "3",
    "ADTO 13º": "4",
    "RESCISAO": "5",
    "PENSAO": "6",
    "PRO LABORE E ESTAGIO": "7"