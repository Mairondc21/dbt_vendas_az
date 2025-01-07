WITH source AS (
    SELECT
        id_marca AS pk_mr_id,
        nome_marca AS mr_nome
    FROM
        {{ source('raw','tb_marca') }}
)
SELECT
    *
FROM
    source