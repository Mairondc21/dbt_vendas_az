WITH source AS(
    SELECT
        id_venda AS pk_vd_id,
        fk_venda_usuario,
        fk_venda_loja,
        data_venda  AS vd_dt_venda,
        valor_total AS vd_valor_total
    FROM
        {{ source('raw','tb_vendas') }}
)
SELECT
    *
FROM
    source