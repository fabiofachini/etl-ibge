
with int_classe_social_massa as (
    select 
        data,
        'Massa Salarial' AS fonte,
        SUM(CASE WHEN classes_sociais_percentil = 'Até o P5' THEN massa_salarial_por_classe_social ELSE 0 END) AS ate_p5,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P5 até o P10' THEN massa_salarial_por_classe_social ELSE 0 END) AS p5_p10,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P10 até o P20' THEN massa_salarial_por_classe_social ELSE 0 END) AS p10_p20,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P20 até o P30' THEN massa_salarial_por_classe_social ELSE 0 END) AS p20_p30,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P30 até o P40' THEN massa_salarial_por_classe_social ELSE 0 END) AS p30_p40,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P40 até o P50' THEN massa_salarial_por_classe_social ELSE 0 END) AS p40_p50,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P50 até o P60' THEN massa_salarial_por_classe_social ELSE 0 END) AS p50_p60,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P60 até o P70' THEN massa_salarial_por_classe_social ELSE 0 END) AS p60_p70,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P70 até o P80' THEN massa_salarial_por_classe_social ELSE 0 END) AS p70_p80,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P80 até o P90' THEN massa_salarial_por_classe_social ELSE 0 END) AS p80_p90,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P90 até o P95' THEN massa_salarial_por_classe_social ELSE 0 END) AS p90_p95,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P95 até o P99' THEN massa_salarial_por_classe_social ELSE 0 END) AS p95_p99,
        SUM(CASE WHEN classes_sociais_percentil = 'Maior que o P99' THEN massa_salarial_por_classe_social ELSE 0 END) AS maior_p99
    FROM {{ ref('stg_ibge__massa_salarial_por_classe_social') }}
    GROUP BY data
)
select * from int_classe_social_massa;
