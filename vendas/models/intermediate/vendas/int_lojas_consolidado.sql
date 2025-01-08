WITH source AS(
    SELECT
        lj.pk_lj_id,
        lj.lj_nome,
        lo.pk_lo_id,
        lo.lo_cidade,
        lo.lo_estado,
        lo.lo_regiao,
        lj.lj_dt_abertura
    FROM
        {{ ref('stg_vendas__lojas') }} lj
    INNER JOIN {{ ref('stg_vendas__localidades') }} lo ON lj.fk_lojas_localidade = lo.pk_lo_id
)

SELECT
    *
FROM
    source