-- models/intermediate/mart_inflacao.sql
with int_inpc_concat_mes as (
    select * from {{ ref('int_inpc_concat_mes') }}
),

int_inpc_concat_ano as (
    select * from {{ ref('int_inpc_concat_ano') }}
),

int_inpc_concat_12m as (
    select * from {{ ref('int_inpc_concat_12m') }}
),

int_ipca_concat_mes as (
    select * from {{ ref('int_ipca_concat_mes') }}
),

int_ipca_concat_ano as (
    select * from {{ ref('int_ipca_concat_ano') }}
),

int_ipca_concat_12m as (
    select * from {{ ref('int_ipca_concat_12m') }}
),

-- transformação dos dados
int_inpc_concat_transformed as (
    select  
    inpc_m.data,
    inpc_m.inpc_ate_2019 AS inpc_mes,
    inpc_a.inpc_ate_2019 AS inpc_ano,
    inpc_12.inpc_ate_2019 AS inpc_12m

    from int_inpc_concat_mes inpc_m
    
    left join int_inpc_concat_ano inpc_a on inpc_m.data = inpc_a.data
    left join int_inpc_concat_12m inpc_12 on inpc_m.data = inpc_12.data
),

int_ipca_concat_transformed as (
    select  
    ipca_m.data,
    ipca_m.ipca_ate_2019 AS ipca_mes,
    ipca_a.ipca_ate_2019 AS ipca_ano,
    ipca_12.ipca_ate_2019 AS ipca_12m

    from int_ipca_concat_mes ipca_m
    
    left join int_ipca_concat_ano ipca_a on ipca_m.data = ipca_a.data
    left join int_ipca_concat_12m ipca_12 on ipca_m.data = ipca_12.data
),

inflacao as (
    select 
        a.data as data,
        a.inpc_mes,
        a.inpc_ano,
        a.inpc_12m,
        b.ipca_mes,
        b.ipca_ano,
        b.ipca_12m


    from int_inpc_concat_transformed a

    left join int_ipca_concat_transformed b on a.data = b.data

)

-- Retorno dos dados
select * from inflacao;
