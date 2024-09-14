-- models/marts/mart_rendimento.sql

with int_rendimento_atividade as (
    select * from {{ ref('int_rendimento_atividade') }}
),

int_rendimento_posicao as (
    select * from {{ ref('int_rendimento_posicao') }}
),

stg_ibge__rendimento_trabalho_principal as (
    select * from {{ ref('stg_ibge__rendimento_trabalho_principal') }}
),

stg_ibge__rendimento_todos_os_trabalhos as (
    select * from {{ ref('stg_ibge__rendimento_todos_os_trabalhos') }}
),

stg_ibge__massa_salarial_habitualmente as (
    select * from {{ ref('stg_ibge__massa_salarial_habitualmente') }}
),

-- CTE para combinar todas as tabelas
rendimento as (
    select 
        a.Data,
        a.Agricultura,
        a.Indústria,
        a.Construção,
        a.Comércio,
        a.Transporte,
        a.Alojamento,
        a.[Informação, Comunic., Financ., Adm.],
        a.[Administração Pública, Saúde, Educação],
        a.[Outros Serviços],
        a.[Serviços Domésticos],
        a.[Total Atividade],
        p.[Conta própria],
        p.Empregado,
        p.[Empregado no setor público],
        p.Empregador,
        p.[Total Posição],
        b.Rendimento_Trabalho_Principal,
        c.Rendimento_Todos_os_Trabalhos,
        d.Massa_Salarial_Habitualmente
    from int_rendimento_atividade a
    join int_rendimento_posicao p on a.Data = p.Data
    join stg_ibge__rendimento_trabalho_principal b on a.Data = b.Data
    join stg_ibge__rendimento_todos_os_trabalhos c on a.Data = c.Data
    join stg_ibge__massa_salarial_habitualmente d on a.Data = d.Data
)

-- Retorno dos dados combinados
select * from rendimento;
