-- models/intermediate/int_ipca_concat_ano.sql

with stg_ibge__ipca_ate_2019 as (
    select * from {{ ref('stg_ibge__ipca_ate_2019') }}
),

stg_ibge__ipca_depois_2019 as (
    select * from {{ ref('stg_ibge__ipca_depois_2019') }}
),

-- transformação dos dados
stg_ibge__ipca_concat as (
    select * from stg_ibge__ipca_ate_2019
    UNION ALL
    select * from stg_ibge__ipca_depois_2019
),

int_ipca_concat_ano as (
    select * from stg_ibge__ipca_concat
    where variavel = 'IPCA - Variação acumulada no ano'
)

-- retorno dos dados
select * from int_ipca_concat_ano