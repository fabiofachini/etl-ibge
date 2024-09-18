with stg_ibge__populacao_economicamente_ativa_f as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where condição_forca_trabalho = 'Força de trabalho'
),

stg_ibge__populacao_economicamente_ativa_fto as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where condição_forca_trabalho = 'Força de trabalho - ocupada'
),

stg_ibge__populacao_economicamente_ativa_ftd as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where condição_forca_trabalho = 'Força de trabalho - desocupada'
),

stg_ibge__populacao_economicamente_ativa_fo as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where condição_forca_trabalho = 'Fora da força de trabalho'
),

stg_ibge__populacao_economicamente_ativa_t as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where condição_forca_trabalho = 'Total'
),

stg_ibge__populacao_mensal as (
    select * from {{ ref('stg_ibge__populacao_mensal') }}
),

stg_ibge__taxa_de_desalentados as (
    select * from {{ ref('stg_ibge__taxa_de_desalentados') }}
),

stg_ibge__taxa_de_desocupacao as (
    select * from {{ ref('stg_ibge__taxa_de_desocupacao') }}
),

stg_ibge__nivel_de_desocupacao as (
    select * from {{ ref('stg_ibge__nivel_de_desocupacao') }}
),

stg_ibge__taxa_de_informalidade as (
    select * from {{ ref('stg_ibge__taxa_de_informalidade') }}
),

stg_ibge__taxa_de_part_forca_de_trabalho as (
    select * from {{ ref('stg_ibge__taxa_de_part_forca_de_trabalho') }}
),

stg_ibge__taxa_de_subocupacao as (
    select * from {{ ref('stg_ibge__taxa_de_subocupacao') }}
),

-- transformação dos dados
populacao_trabalho_joined as (
    select 
        f.data as data,
        f.populacao_economicamente_ativa as forca_trabalho,
        fto.populacao_economicamente_ativa as forca_trabalho_ocupada,
        ftd.populacao_economicamente_ativa as forca_trabalho_desocupada,
        fo.populacao_economicamente_ativa as fora_forca_trabalho,
        t.populacao_economicamente_ativa as forca_trabalho_total,
        pp.populacao as população_total,
        d.taxa_de_desalentados,
        de.taxa_de_desocupacao,
        des.nivel_de_desocupacao,
        i.taxa_de_informalidade,
        p.taxa_de_part_forca_de_trabalho,
        s.taxa_de_subocupacao

    from stg_ibge__populacao_economicamente_ativa_f f

    left join stg_ibge__populacao_economicamente_ativa_fto fto on f.data = fto.data
    left join stg_ibge__populacao_economicamente_ativa_ftd ftd on f.data = ftd.data
    left join stg_ibge__populacao_economicamente_ativa_fo fo on f.data = fo.data
    left join stg_ibge__populacao_economicamente_ativa_t t on f.data = t.data
    left join stg_ibge__populacao_mensal pp on f.data = pp.data
    
    left join stg_ibge__taxa_de_desalentados d on f.data = d.data
    left join stg_ibge__taxa_de_desocupacao de on f.data = de.data
    left join stg_ibge__nivel_de_desocupacao des on f.data = des.data
    left join stg_ibge__taxa_de_informalidade i on f.data = i.data
    left join stg_ibge__taxa_de_part_forca_de_trabalho p on f.data = p.data
    left join stg_ibge__taxa_de_subocupacao s on f.data = s.data
)

-- retorno dos dados
select * from populacao_trabalho_joined;
