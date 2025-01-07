WITH source AS (
    SELECT
        id_usuario AS pk_us_id,
        nome_usuario AS us_nome,
        email AS us_email,
        navegador AS us_navegador,
        data_nascimento AS us_dt_nascimento,
        data_cadastro AS us_dt_cadastro,
        fk_usuarios_localidade
    FROM
        {{ source('raw','tb_usuarios') }}
)
SELECT
    *
FROM
    source