WITH source AS (
    SELECT
        pk_pr_id AS sk_dim_produtos,
        pr_nome,
        mr_nome AS pr_mr_nome,
        pr_categoria
    FROM
        {{ ref('int_produtos_consolidado') }}
    ORDER BY pk_pr_id ASC

)
SELECT
    *
FROM
    source