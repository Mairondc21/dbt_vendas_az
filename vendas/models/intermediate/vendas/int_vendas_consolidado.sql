WITH source AS (
    SELECT
        vd.pk_vd_id,
        us.pk_us_id,
        us.us_nome,
        lj.pk_lj_id,
        lj.lj_nome,
        DATE_PART('YEAR',vd.vd_dt_venda) AS vd_ano_venda,
        DATE_PART('MONTH',vd.vd_dt_venda) AS vd_mes_venda,
        DATE_PART('DAY',vd.vd_dt_venda) AS vd_dia_venda
    FROM
        {{ ref('stg_vendas__vendas') }} vd
    INNER JOIN {{ ref('int_usuarios_consolidado') }} us ON us.pk_us_id = vd.fk_venda_usuario
    INNER JOIN {{ ref('int_lojas_consolidado') }} lj ON lj.pk_lj_id = vd.fk_venda_loja
),
itens_venda AS (
    SELECT
        iv.pk_iv_id,
        sr.pk_vd_id AS fk_vd_id,
        pr.pk_pr_id,
        pr.pr_nome,
        iv.iv_quantidade,
        pr.pr_preco,
        ROUND(CAST(pr.pr_preco * iv.iv_quantidade AS NUMERIC), 2) as iv_preco_total
    FROM
        {{ ref('stg_vendas__itens_venda') }} iv
    INNER JOIN source sr ON sr.pk_vd_id = iv.fk_item_venda_vendas
    INNER JOIN {{ ref('int_produtos_consolidado') }} pr ON pr.pk_pr_id = iv.fk_item_venda_produto
),
vendas_final AS (
    SELECT
        iv.pk_iv_id,
        sr.pk_us_id,
        sr.us_nome,
        iv.pk_pr_id,
        iv.pr_nome,
        sr.pk_lj_id,
        sr.lj_nome,
        iv.iv_quantidade,
        iv.pr_preco,
        iv.iv_preco_total,
        sr.vd_dia_venda,
        sr.vd_mes_venda,
        sr.vd_ano_venda,
        ROUND(SUM(iv_preco_total) OVER( PARTITION BY sr.pk_us_id,sr.vd_dia_venda),2) AS vd_valor_total
    from itens_venda iv
    INNER JOIN source sr ON iv.fk_vd_id = sr.pk_vd_id
    ORDER BY iv.pk_iv_id ASC
) 
SELECT
    *
FROM
    vendas_final      