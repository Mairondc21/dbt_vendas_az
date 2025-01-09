WITH source AS (
    SELECT
        id_item_venda AS pk_iv_id,
        fk_item_venda_vendas,
        fk_item_venda_produto,
        quantidade AS iv_quantidade
    FROM
        {{ source('raw','tb_item_venda') }}
)
SELECT
    *
FROM 
    source