-- 1. Evolução mensal de Pedro Alves (valores monetários em centavos).
SELECT competencia, receita_centavos, custo_centavos, lucro_centavos,
       margem_percentual, itens
FROM gold_margem_consultor
WHERE cod_vendedor = 'V03'
ORDER BY competencia;

-- 2. Devoluções por documento e cliente. Os valores negativos são preservados.
SELECT nr_documento, cod_cliente, MAX(nome_cliente) AS nome_cliente,
       COUNT(*) AS itens,
       SUM(receita_centavos) AS receita_centavos,
       SUM(custo_centavos) AS custo_centavos,
       SUM(receita_centavos - custo_centavos) AS lucro_centavos
FROM gold_faturamento_item
WHERE cod_vendedor = 'V03' AND tipo_documento = 'DEVOLUCAO'
GROUP BY nr_documento, cod_cliente
ORDER BY ABS(SUM(receita_centavos)) DESC;

-- 3. Mix, margem e participação por grupo.
WITH grupos AS (
    SELECT competencia, cod_vendedor, grupo_material,
           SUM(receita_centavos) AS receita_centavos,
           SUM(custo_centavos) AS custo_centavos
    FROM gold_faturamento_item
    WHERE cod_vendedor = 'V03'
    GROUP BY competencia, cod_vendedor, grupo_material
), totais AS (
    SELECT competencia, cod_vendedor,
           SUM(receita_centavos) AS receita_total_centavos
    FROM grupos
    GROUP BY competencia, cod_vendedor
)
SELECT g.competencia, g.cod_vendedor, g.grupo_material,
       g.receita_centavos, g.custo_centavos,
       g.receita_centavos - g.custo_centavos AS lucro_centavos,
       CASE WHEN g.receita_centavos = 0 THEN NULL
            ELSE 100.0 * (g.receita_centavos - g.custo_centavos)
                 / g.receita_centavos END AS margem_percentual,
       CASE WHEN t.receita_total_centavos = 0 THEN NULL
            ELSE 100.0 * g.receita_centavos
                 / t.receita_total_centavos END AS participacao_receita_percentual
FROM grupos AS g
INNER JOIN totais AS t
    ON g.competencia = t.competencia
   AND g.cod_vendedor = t.cod_vendedor
ORDER BY g.competencia, g.grupo_material;

-- 4. Contrafactual: outubro sem somente a devolução 404622.
SELECT CASE WHEN SUM(receita_centavos) = 0 THEN NULL
            ELSE 100.0 * (SUM(receita_centavos) - SUM(custo_centavos))
                 / SUM(receita_centavos) END AS margem_sem_404622
FROM gold_faturamento_item
WHERE competencia = '2026-10'
  AND cod_vendedor = 'V03'
  AND nr_documento <> '404622';
