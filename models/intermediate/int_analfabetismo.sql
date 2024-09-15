-- models/intermediate/int_analfabetismo_joined.sql

with stg_ibge__taxa_de_analfabetismo as (
    select * from {{ ref('stg_ibge__taxa_de_analfabetismo') }}
),

-- transformação dos dados
stg_ibge__taxa_de_analfabetismo_m as (
    select 
        data,
        taxa_de_analfabetismo as taxa_de_analfabetismo_mulheres
    from stg_ibge__taxa_de_analfabetismo
    where sexo = 'mulheres'
),

stg_ibge__taxa_de_analfabetismo_h as (
    select 
        data,
        taxa_de_analfabetismo as taxa_de_analfabetismo_homens
    from stg_ibge__taxa_de_analfabetismo
    where sexo = 'homens'
),

stg_ibge__taxa_de_analfabetismo_t as (
    select 
        data,
        taxa_de_analfabetismo as taxa_de_analfabetismo_total
    from stg_ibge__taxa_de_analfabetismo
    where sexo = 'total'
),

int_analfabetismo_joined as (
    select 
        m.data as data,
        m.taxa_de_analfabetismo_mulheres,
        h.taxa_de_analfabetismo_homens,
        t.taxa_de_analfabetismo_total
        
    from stg_ibge__taxa_de_analfabetismo_m m

    left join stg_ibge__taxa_de_analfabetismo_h h on m.data = h.data
    left join stg_ibge__taxa_de_analfabetismo_t t on m.data = t.data
)

-- retorno dos dados
select * from int_analfabetismo_joined;
