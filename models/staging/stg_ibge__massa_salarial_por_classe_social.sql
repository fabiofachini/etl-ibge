-- models/staging/stg_ibge__massa_salarial_por_classe_social.sql

with massa_salarial_por_classe_social as (
    select * from {{ source('ibge', 'massa_salarial_por_classe_social') }}
),

-- transformação dos dados
stg_ibge__massa_salarial_por_classe_social as (
    select
        CONVERT(DATE, 
            [Ano (Código)] + '-01-01') AS data,
        TRY_CAST(
        CASE 
            WHEN [Valor] = '...' THEN NULL
            ELSE [Valor]
        END AS NUMERIC(10,4)
    ) AS massa_salarial_por_classe_social,
        [Classes de percentual das pessoas em ordem crescente de rendimento domiciliar per capita] AS classes_sociais_percentil
    from massa_salarial_por_classe_social
)  
-- retorno dos dados transformados
select * from stg_ibge__massa_salarial_por_classe_social
