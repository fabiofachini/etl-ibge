-- models/staging/stg_ibge__inpc_depois_2019.sql

with inpc_depois_2019 as (
    select * from {{ source('ibge', 'inpc_depois_2019') }}
),

-- transformação dos dados
stg_ibge__inpc_depois_2019 as (
    select
        CONVERT(DATE, 
            SUBSTRING(CAST([Mês (Código)] AS VARCHAR(6)), 1, 4) + '-' + 
            SUBSTRING(CAST([Mês (Código)] AS VARCHAR(6)), 5, 2) + '-01') AS data,
        TRY_CAST(
        CASE 
            WHEN [Valor] = '...' THEN NULL
            ELSE [Valor]
        END AS NUMERIC(10,2)
    ) AS inpc_depois_2019,
    Variável AS variavel
    from inpc_depois_2019
)

-- retorno dos dados transformados
select * from stg_ibge__inpc_depois_2019
