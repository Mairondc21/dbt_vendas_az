WITH source AS (
    SELECT
        id_loja AS pk_lj_id,
        nome_loja AS lj_nome,
        fk_lojas_localidade,
        data_abertura AS lj_dt_abertura
    FROM
        {{ source('raw','tb_lojas') }}
)
SELECT 
    *
FROM
    source