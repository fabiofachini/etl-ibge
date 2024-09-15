-- models/staging/stg_ibge__rendimento_classe_social.sql

with rendimento_classe_social as (
    select * from {{ source('ibge', 'rendimento_classe_social') }}
),

-- transformação dos dados
stg_ibge__rendimento_classe_social as (
    select
        CONVERT(DATE, 
            [Ano (Código)] + '-01-01') AS data,
        TRY_CAST(
        CASE 
            WHEN [Valor] = '...' THEN NULL
            ELSE [Valor]
        END AS INT) AS rendimento_classe_social,
        [Classes de percentual das pessoas em ordem crescente de rendimento domiciliar per capita] AS classes_sociais_percentil
    from rendimento_classe_social
)

-- retorno dos dados transformados
select * from stg_ibge__rendimento_classe_social
