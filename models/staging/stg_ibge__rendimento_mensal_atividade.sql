-- models/staging/stg_ibge__rendimento_mensal_atividade.sql

with rendimento_mensal_atividade as (
    select * from {{ source('ibge', 'rendimento_mensal_atividade') }}
),

-- transformação dos dados
stg_ibge__rendimento_mensal_atividade as (
    select
        CONVERT(DATE, 
            SUBSTRING(CAST([Trimestre Móvel (Código)] AS VARCHAR(6)), 1, 4) + '-' + 
            SUBSTRING(CAST([Trimestre Móvel (Código)] AS VARCHAR(6)), 5, 2) + '-01') AS data,
        TRY_CAST(
        CASE 
            WHEN [Valor] = '...' THEN NULL
            ELSE [Valor]
        END AS INT
    ) AS rendimento_mensal_atividade,
    [Grupamento de atividade no trabalho principal] AS trabalho_principal
    from rendimento_mensal_atividade
)
-- retorno dos dados transformados
select * from stg_ibge__rendimento_mensal_atividade
