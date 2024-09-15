-- models/marts/fato_instrucao.sql

with stg_ibge__nivel_de_instrucao as (
    select * from {{ ref('stg_ibge__nivel_de_instrucao') }}
),

-- transformação dos dados
instrucao as (
    SELECT 
        data,
        SUM(CASE WHEN nivel_de_instrucao = 'Sem instrução e fundamental incompleto ou equivalente' THEN mil_pessoas ELSE 0 END) AS 'sem_instrucao',
        SUM(CASE WHEN nivel_de_instrucao = 'Fundamental completo e médio incompleto ou equivalente' THEN mil_pessoas ELSE 0 END) AS 'fundamental',
        SUM(CASE WHEN nivel_de_instrucao = 'Médio completo ou equivalente e superior incompleto' THEN mil_pessoas ELSE 0 END) AS 'medio',
        SUM(CASE WHEN nivel_de_instrucao = 'Superior completo' THEN mil_pessoas ELSE 0 END) AS 'superior'
    FROM stg_ibge__nivel_de_instrucao 
    WHERE Sexo = 'Total'
    GROUP BY data
)

-- retorno dos dados
select * from instrucao