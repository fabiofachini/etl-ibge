-- models/marts/fato_rendimento_posicao.sql

with stg_ibge__rendimento_mensal_posicao as (
    select * from {{ ref('stg_ibge__rendimento_mensal_posicao') }}
),

-- transformação dos dados

rendimento_posicao as (
    SELECT 
        data,
        SUM(CASE WHEN posicao_trabalho = 'Conta própria' THEN rendimento_mensal_posicao ELSE 0 END) AS 'conta_propria',
        SUM(CASE WHEN posicao_trabalho = 'Empregado' THEN rendimento_mensal_posicao ELSE 0 END) AS 'empregado',
        SUM(CASE WHEN posicao_trabalho = 'Empregado no setor público' THEN rendimento_mensal_posicao ELSE 0 END) AS 'empregado_no_setor_publico',
        SUM(CASE WHEN posicao_trabalho = 'Empregador' THEN rendimento_mensal_posicao ELSE 0 END) AS 'empregador',
        SUM(CASE WHEN posicao_trabalho = 'Total' THEN rendimento_mensal_posicao ELSE 0 END) AS 'total_posicao'
    FROM stg_ibge__rendimento_mensal_posicao
    GROUP BY data
)


-- retorno dos dados
select * from rendimento_posicao