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
        a.data as data,
        a.taxa_de_analfabetismo_mulheres,
        a.taxa_de_analfabetismo_homens,
        a.taxa_de_analfabetismo_total,
        b.sem_instrucao,
        b.fundamental,
        b.medio,
        b.superior



    from int_analfabetismo a

    left join int_instrucao b on a.data = b.data

)

-- Retorno dos dados
select * from educacao;
