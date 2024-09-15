-- models/staging/stg_ibge__rendimento_trabalho_principal.sql

with rendimento_trabalho_principal as (
    select * from {{ source('ibge', 'rendimento_trabalho_principal') }}
),

-- transformação dos dados
stg_ibge__rendimento_trabalho_principal as (
    select
        CONVERT(DATE, 
            SUBSTRING(CAST([Trimestre Móvel (Código)] AS VARCHAR(6)), 1, 4) + '-' + 
            SUBSTRING(CAST([Trimestre Móvel (Código)] AS VARCHAR(6)), 5, 2) + '-01') AS data,
        TRY_CAST(
        CASE 
            WHEN [Valor] = '...' THEN NULL
            ELSE [Valor]
        END AS INT
    ) AS rendimento_trabalho_principal
    from rendimento_trabalho_principal
)

-- retorno dos dados transformados
select * from stg_ibge__rendimento_trabalho_principal
