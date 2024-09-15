-- models/staging/stg_ibge__taxa_de_informalidade.sql

with taxa_de_informalidade as (
    select * from {{ source('ibge', 'taxa_de_informalidade') }}
),

-- transformação dos dados
stg_ibge__taxa_de_informalidade as (
    select
        CONVERT(DATE, 
            SUBSTRING(CAST([Trimestre Móvel (Código)] AS VARCHAR(6)), 1, 4) + '-' + 
            SUBSTRING(CAST([Trimestre Móvel (Código)] AS VARCHAR(6)), 5, 2) + '-01') AS data,
        TRY_CAST(
        CASE 
            WHEN [Valor] = '...' THEN NULL
            ELSE [Valor]
        END AS NUMERIC(10,1)
    ) AS taxa_de_informalidade
    from taxa_de_informalidade
)

-- retorno dos dados transformados
select * from stg_ibge__taxa_de_informalidade
