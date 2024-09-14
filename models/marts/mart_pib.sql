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
        u.Data as Data,
        v.PIB_Variacao_Trimestral,
        a.PIB_Anual as PIB_Anual_PC,
        b.PIB_Anual as PIB_Anual,
        c.Populacao_Anual as Populacao_Anual
    from unified_dates u
    left join stg_pib_variacao_trimestral v on u.Data = v.Data
    left join stg_pib_anual_pc a on u.Data = a.Data
    left join stg_pib_anual b on u.Data = b.Data
    left join stg_ibge__populacao_anual c on u.Data = c.Data
)

-- Retorno dos dados
select * from trabalho_joined;
