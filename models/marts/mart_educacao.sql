-- models/intermediate/mart_pib.sql

with int_analfabetismo as (
    select * from {{ ref('int_analfabetismo') }}
),

int_instrucao as (
    select * from {{ ref('int_instrucao') }}
),

-- Transformação dos dados
educacao as (
    select 
        a.Data as Data,
        a.Taxa_de_Analfabetismo_Mulheres,
        a.Taxa_de_Analfabetismo_Homens,
        a.Taxa_de_Analfabetismo_Total,
        b.sem_instrucao,
        b.Fundamental,
        b.Médio,
        b.Superior



    from int_analfabetismo a

    left join int_instrucao b on a.Data = b.Data

)

-- Retorno dos dados
select * from educacao;
