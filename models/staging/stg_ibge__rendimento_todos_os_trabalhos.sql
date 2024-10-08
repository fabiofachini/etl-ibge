-- models/staging/stg_ibge__rendimento_todos_os_trabalhos.sql

with rendimento_todos_os_trabalhos as (
    select * from {{ source('ibge', 'rendimento_todos_os_trabalhos') }}
),

-- transformação dos dados
stg_ibge__rendimento_todos_os_trabalhos as (
    select
        CONVERT(DATE, 
            SUBSTRING(CAST([Trimestre Móvel (Código)] AS VARCHAR(6)), 1, 4) + '-' + 
            SUBSTRING(CAST([Trimestre Móvel (Código)] AS VARCHAR(6)), 5, 2) + '-01') AS data,
        TRY_CAST(
        CASE 
            WHEN [Valor] = '...' THEN NULL
            ELSE [Valor]
        END AS INT
    ) AS rendimento_todos_os_trabalhos
    from rendimento_todos_os_trabalhos
)

-- retorno dos dados transformados
select * from stg_ibge__rendimento_todos_os_trabalhos
