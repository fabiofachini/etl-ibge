-- models/staging/stg_ibge__nivel_de_instrucao.sql

with nivel_de_instrucao as (
    select * from {{ source('ibge', 'nivel_de_instrucao') }}
),

-- transformação dos dados
stg_ibge__nivel_de_instrucao as (
    select
        CONVERT(DATE, 
            [Ano (Código)] + '-01-01') AS data,
        TRY_CAST(
        CASE 
            WHEN [Valor] = '...' THEN NULL
            ELSE [Valor]
        END AS INT) AS mil_pessoas,
        sexo,
        [Nível de instrução] AS nivel_de_instrucao
    from nivel_de_instrucao
)

-- retorno dos dados transformados
select * from stg_ibge__nivel_de_instrucao
