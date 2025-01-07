WITH source AS (
    SELECT
        id_produto AS pk_pr_id
        nome_produto AS pr_nome
        fk_produtos_marca,
        preco AS pr_preco,
        categoria  AS pr_categoria
    FROM
        {{ source('raw','tb_produtos') }}
)
SELECT
    *
FROM
    source