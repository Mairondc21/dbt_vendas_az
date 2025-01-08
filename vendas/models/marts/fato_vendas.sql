WITH source AS (
    SELECT
        pk_iv_id AS ft_id,
        us.sk_dim AS sk_dim_usuarios,
        pr.sk_dim AS sk_dim_produtos,
        lj.sk_dim AS sk_dim_lojas,
        vd.iv_quantidade AS quantidade,
        vd.iv_preco_unitario AS preco_uni,
        vd.iv_preco_total AS preco_total,
        vd.vd_dia_venda AS dia_venda,
        vd.vd_mes_venda AS mes_venda,
        vd.vd_ano_venda AS ano_venda,
        vd.vd_valor_total AS total_venda
    FROM
        {{ ref('int_vendas_consolidado') }} vd
    INNER JOIN {{ref('dim_usuarios')}} us ON us.sk_dim = vd.pk_us_id
    INNER JOIN {{ref('dim_produtos')}} pr ON pr.sk_dim = vd.pk_pr_id
    INNER JOIN {{ref('dim_lojas')}} lj ON lj.sk_dim = vd.pk_lj_id
    ORDER BY  pk_iv_id

)
SELECT
    *
FROM
    source