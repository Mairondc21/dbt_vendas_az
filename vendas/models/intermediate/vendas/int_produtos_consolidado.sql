WITH source AS (
    SELECT
        pk_pr_id,
        pr_nome,
        pk_mr_id,
        mr_nome,
        CAST(pr_preco AS NUMERIC) AS pr_preco ,
        pr_categoria
    FROM
        {{ ref('stg_vendas__produtos') }} pr
    INNER JOIN {{ ref('stg_vendas__marcas') }} mr ON mr.pk_mr_id = pr.fk_produtos_marca
)
SELECT
    *
FROM
    source