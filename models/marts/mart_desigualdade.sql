-- models/marts/mart_desigualdade.sql

with int_classe_social_massa as (
    select * from {{ ref('int_classe_social_massa') }}
),

int_classe_social_pop as (
    select * from {{ ref('int_classe_social_pop') }}
),

int_classe_social_renda as (
    select * from {{ ref('int_classe_social_renda') }}
),

stg_ibge__indice_de_gini as (
    select * from {{ ref('stg_ibge__indice_de_gini') }}
),

-- CTE para combinar todas as tabelas
combined_data as (
    select * from int_classe_social_massa
    union all
    select * from int_classe_social_pop
    union all
    select * from int_classe_social_renda
),

-- Junção dos dados combinados com o índice de Gini
combined_data_with_gini as (
    select 
        c.*,
        i.indice_gini
    from combined_data c
    left join stg_ibge__indice_de_gini i
    on c.Data = i.Data AND c.fonte = 'Renda'
)

-- Retorno dos dados combinados com índice de Gini
select * from combined_data_with_gini;

