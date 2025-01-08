WITH source AS (
    SELECT
        pk_us_id AS sk_dim,
        us_nome,
        us_navegador,
        us_idade,
        lo_cidade AS us_lo_cidade,
        lo_estado AS us_lo_estado,
        lo_regiao AS us_lo_regiao
    FROM
        {{ ref('int_usuarios_consolidado') }}
    ORDER BY pk_us_id
)
SELECT
    *
FROM
    source