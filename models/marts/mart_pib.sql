-- models/intermediate/mart_pib.sql

with stg_pib_anual_pc as (
    select * from {{ ref('stg_ibge__pib_anual_pc') }}
),

stg_pib_anual as (
    select * from {{ ref('stg_ibge__pib_anual') }}
),

stg_pib_variacao_trimestral as (
    select * from {{ ref('stg_ibge__pib_variacao_trimestral') }}
),

stg_ibge__populacao_anual as (
    select * from {{ ref('stg_ibge__populacao_anual') }}
),

-- União das datas das tabelas stg_pib_variacao_trimestral e stg_pib_anual
unified_dates as (
    select Data from stg_pib_variacao_trimestral
    union
    select Data from stg_pib_anual
),

-- Junção das tabelas usando as datas unificadas
trabalho_joined as (
    select 
        u.data as data,
        v.pib_variacao_trimestral,
        a.pib_anual as pib_anual_pc,
        b.pib_anual as pib_anual,
        c.populacao_anual as populacao_anual
    from unified_dates u
    left join stg_pib_variacao_trimestral v on u.data = v.data
    left join stg_pib_anual_pc a on u.data = a.data
    left join stg_pib_anual b on u.data = b.data
    left join stg_ibge__populacao_anual c on u.data = c.data
)

-- Retorno dos dados
select * from trabalho_joined;
