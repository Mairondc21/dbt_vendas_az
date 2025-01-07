WITH source AS (
    SELECT
        id_item_venda AS pk_iv_id
        fk_item_venda_vendas,
        fk_item_venda_produto,
        quantidade AS iv_quantidade,
        preco_unitario AS iv_preco_unitario,
        preco_total AS iv_preco_total
)