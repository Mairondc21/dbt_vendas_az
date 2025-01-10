WITH source AS (
    SELECT
        pk_iv_id AS ft_id,
        us.sk_dim_usuarios,
        pr.sk_dim_produtos,
        lj.sk_dim_lojas,
        vd.iv_quantidade AS quantidade,
        vd.pr_preco AS preco_uni,
        vd.iv_preco_total AS preco_total,
        vd.vd_mes_venda AS mes_venda,
        vd.vd_ano_venda AS ano_venda,
        vd.vd_valor_total AS total_venda
    FROM
        {{ ref('int_vendas_consolidado') }} vd
    INNER JOIN {{ref('snap_dim_usuarios')}} us ON us.sk_dim_usuarios = vd.pk_us_id
    INNER JOIN {{ref('dim_produtos')}} pr ON pr.sk_dim_produtos = vd.pk_pr_id
    INNER JOIN {{ref('snap_dim_lojas')}} lj ON lj.sk_dim_lojas = vd.pk_lj_id
    WHERE us.dbt_valid_to IS NULL AND lj.dbt_valid_to IS NULL
    ORDER BY  pk_iv_id

)
SELECT
    *
FROM
    source