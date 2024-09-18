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
        a.data,
        a.agricultura,
        a.industria,
        a.construcao,
        a.comercio,
        a.transporte,
        a.alojamento,
        a.informacao_comunic_financ_adm,
        a.administracao_publica_saude_educacao,
        a.outros_servicos,
        a.servicos_domesticos,
        a.total_atividade,
        p.conta_propria,
        p.empregado,
        p.empregado_no_setor_publico,
        p.empregador,
        p.total_posicao,
        b.rendimento_trabalho_principal,
        c.rendimento_todos_os_trabalhos,
        d.massa_salarial_habitualmente
    from int_rendimento_atividade a
    join int_rendimento_posicao p on a.data = p.data
    join stg_ibge__rendimento_trabalho_principal b on a.data = b.data
    join stg_ibge__rendimento_todos_os_trabalhos c on a.data = c.data
    join stg_ibge__massa_salarial_habitualmente d on a.data = d.data
)

-- Retorno dos dados combinados
select * from rendimento;
