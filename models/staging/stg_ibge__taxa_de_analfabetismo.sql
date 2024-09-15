-- models/staging/stg_ibge__taxa_de_analfabetismo.sql

with taxa_de_analfabetismo as (
    select * from {{ source('ibge', 'taxa_de_analfabetismo') }}
),

-- transformação dos dados
stg_ibge__taxa_de_analfabetismo as (
    select
        CONVERT(DATE, 
            [Ano (Código)] + '-01-01') AS data,
        TRY_CAST(
        CASE 
            WHEN [Valor] = '...' THEN NULL
            ELSE [Valor]
        END AS NUMERIC(10,1)) AS taxa_de_analfabetismo,
        sexo
    from taxa_de_analfabetismo
)

-- retorno dos dados transformados
select * from stg_ibge__taxa_de_analfabetismo
