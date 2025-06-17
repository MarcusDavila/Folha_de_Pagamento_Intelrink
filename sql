INSERT INTO contaapagar_composicao (
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
    0 AS reduzido,                                   
    REPLACE(REPLACE(valor, '.', ''), ',', '.')::NUMERIC(15,2) AS valortitulo, 
    1 AS quantidadeparcela,                          
    0.00 AS valortitulopendente,                     
    0.00 AS valortituloutilizado                    
FROM folha_pagamento;                                 

