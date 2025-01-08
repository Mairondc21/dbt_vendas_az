WITH source AS (
    SELECT
       us.pk_us_id,
       us.us_nome,
       us.us_navegador,
       2025 - DATE_PART('year',us.us_dt_nascimento)  AS us_idade,
       us.us_dt_cadastro,
       lo.pk_lo_id,
       lo.lo_cidade,
       lo.lo_estado,
       lo.lo_regiao
    FROM
        {{ ref('stg_vendas__usuarios') }} us
    INNER JOIN {{ ref('stg_vendas__localidades') }} lo ON us.fk_usuarios_localidade = lo.pk_lo_id   
)
SELECT
    *
FROM
    source