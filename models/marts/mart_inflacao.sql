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
    inpc_m.Data,
    inpc_m.INPC_ate_2019 AS INPC_Mes,
    inpc_a.INPC_ate_2019 AS INPC_Ano,
    inpc_12.INPC_ate_2019 AS INPC_12M

    from int_inpc_concat_mes inpc_m
    
    left join int_inpc_concat_ano inpc_a on inpc_m.Data = inpc_a.Data
    left join int_inpc_concat_12m inpc_12 on inpc_m.Data = inpc_12.Data
),

int_ipca_concat_transformed as (
    select  
    ipca_m.Data,
    ipca_m.IPCA_ate_2019 AS IPCA_Mes,
    ipca_a.IPCA_ate_2019 AS IPCA_Ano,
    ipca_12.IPCA_ate_2019 AS IPCA_12M

    from int_ipca_concat_mes ipca_m
    
    left join int_ipca_concat_ano ipca_a on ipca_m.Data = ipca_a.Data
    left join int_ipca_concat_12m ipca_12 on ipca_m.Data = ipca_12.Data
),

inflacao as (
    select 
        a.Data as Data,
        a.INPC_Mes,
        a.INPC_Ano,
        a.INPC_12M,
        b.IPCA_Mes,
        b.IPCA_Ano,
        b.IPCA_12M


    from int_inpc_concat_transformed a

    left join int_ipca_concat_transformed b on a.Data = b.Data

)

-- Retorno dos dados
select * from inflacao;
