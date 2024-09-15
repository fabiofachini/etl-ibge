-- models/marts/fato_rendimento_atividade.sql

with stg_ibge__rendimento_mensal_atividade as (
    select * from {{ ref('stg_ibge__rendimento_mensal_atividade') }}
),

-- transformação dos dados

rendimento_atividade as (
    SELECT 
        data,
        SUM(CASE WHEN trabalho_principal = 'Agricultura, pecuária, produção florestal, pesca e aquicultura' THEN rendimento_mensal_atividade ELSE 0 END) AS 'agricultura',
        SUM(CASE WHEN trabalho_principal = 'Indústria geral' THEN rendimento_mensal_atividade ELSE 0 END) AS 'industria',
        SUM(CASE WHEN trabalho_principal = 'Construção' THEN rendimento_mensal_atividade ELSE 0 END) AS 'construcao',
        SUM(CASE WHEN trabalho_principal = 'Comércio, reparação de veículos automotores e motocicletas' THEN rendimento_mensal_atividade ELSE 0 END) AS 'comercio',
        SUM(CASE WHEN trabalho_principal = 'Transporte, armazenagem e correio' THEN rendimento_mensal_atividade ELSE 0 END) AS 'transporte',
        SUM(CASE WHEN trabalho_principal = 'Alojamento e alimentação' THEN rendimento_mensal_atividade ELSE 0 END) AS 'alojamento',
        SUM(CASE WHEN trabalho_principal = 'Informação, comunicação e atividades financeiras, imobiliárias, profissionais e administrativas' THEN rendimento_mensal_atividade ELSE 0 END) AS 'informacao,_comunic.,_financ.,_adm.',
        SUM(CASE WHEN trabalho_principal = 'Administração pública, defesa, seguridade social, educação, saúde humana e serviços sociais' THEN rendimento_mensal_atividade ELSE 0 END) AS 'administracao_publica,_saude,_educacao',
        SUM(CASE WHEN trabalho_principal = 'Outros serviços' THEN rendimento_mensal_atividade ELSE 0 END) AS 'outros_servicos',
        SUM(CASE WHEN trabalho_principal = 'Serviços domésticos' THEN rendimento_mensal_atividade ELSE 0 END) AS 'servicos_domesticos',
        SUM(CASE WHEN trabalho_principal = 'Total' THEN rendimento_mensal_atividade ELSE 0 END) AS 'total_atividade'
    FROM stg_ibge__rendimento_mensal_atividade
    GROUP BY data
)

-- retorno dos dados
select * from rendimento_atividade