-- models/intermediate/int_inpc_concat_ano.sql

with stg_ibge__inpc_ate_2019 as (
    select * from {{ ref('stg_ibge__inpc_ate_2019') }}
),

stg_ibge__inpc_depois_2019 as (
    select * from {{ ref('stg_ibge__inpc_depois_2019') }}
),

-- transformação dos dados
stg_ibge__inpc_concat as (
    select * from stg_ibge__inpc_ate_2019
    UNION ALL
    select * from stg_ibge__inpc_depois_2019
),

int_inpc_concat_ano as (
    select * from stg_ibge__inpc_concat
    where variavel = 'INPC - Variação acumulada no ano'
)

-- retorno dos dados
select * from int_inpc_concat_ano