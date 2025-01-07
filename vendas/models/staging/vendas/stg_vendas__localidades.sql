WITH source AS (
    SELECT
        id_localidade AS pk_lo_id,
        nome_cidade AS lo_nome,
        nome_estado AS lo_estado,
        nome_regiao AS lo_regiao
    FROM
        {{ source('raw','tb_localidade') }}
)
SELECT
    *
FROM
    source