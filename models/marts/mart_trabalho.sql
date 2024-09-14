with stg_ibge__populacao_economicamente_ativa_f as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where Condição_Forca_Trabalho = 'Força de Trabalho'
),

stg_ibge__populacao_economicamente_ativa_fto as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where Condição_Forca_Trabalho = 'Força de Trabalho - ocupada'
),

stg_ibge__populacao_economicamente_ativa_ftd as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where Condição_Forca_Trabalho = 'Força de Trabalho - desocupada'
),

stg_ibge__populacao_economicamente_ativa_fo as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where Condição_Forca_Trabalho = 'Fora da força de trabalho'
),

stg_ibge__populacao_economicamente_ativa_t as (
    select * from {{ ref('stg_ibge__populacao_economicamente_ativa') }}
    where Condição_Forca_Trabalho = 'Total'
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
int_populacao_trabalho_joined as (
    select 
        f.Data as Data,
        f.Populacao_Economicamente_Ativa as Forca_Trabalho,
        fto.Populacao_Economicamente_Ativa as Forca_Trabalho_Ocupada,
        ftd.Populacao_Economicamente_Ativa as Forca_Trabalho_Desocupada,
        fo.Populacao_Economicamente_Ativa as Fora_Forca_Trabalho,
        t.Populacao_Economicamente_Ativa as Forca_Trabalho_Total,
        pp.Populacao as População_Total,
        d.Taxa_de_Desalentados,
        de.Taxa_de_Desocupacao,
        des.Nivel_de_Desocupacao,
        i.Taxa_de_Informalidade,
        p.Taxa_de_Part_Forca_de_Trabalho,
        s.Taxa_de_Subocupacao

    from stg_ibge__populacao_economicamente_ativa_f f

    left join stg_ibge__populacao_economicamente_ativa_fto fto on f.Data = fto.Data
    left join stg_ibge__populacao_economicamente_ativa_ftd ftd on f.Data = ftd.Data
    left join stg_ibge__populacao_economicamente_ativa_fo fo on f.Data = fo.Data
    left join stg_ibge__populacao_economicamente_ativa_t t on f.Data = t.Data
    left join stg_ibge__populacao_mensal pp on f.Data = pp.Data
    
    left join stg_ibge__taxa_de_desalentados d on f.Data = d.Data
    left join stg_ibge__taxa_de_desocupacao de on f.Data = de.Data
    left join stg_ibge__nivel_de_desocupacao des on f.Data = des.Data
    left join stg_ibge__taxa_de_informalidade i on f.Data = i.Data
    left join stg_ibge__taxa_de_part_forca_de_trabalho p on f.Data = p.Data
    left join stg_ibge__taxa_de_subocupacao s on f.Data = s.Data
)

-- retorno dos dados
select * from int_populacao_trabalho_joined;
