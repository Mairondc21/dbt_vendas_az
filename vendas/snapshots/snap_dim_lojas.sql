{% snapshot snap_dim_lojas %}
{{
    config(
        target_schema='marts',
        target_database='postgres_user',
        unique_key='sk_dim_lojas'
    )
}}

WITH source AS (
    SELECT
        pk_lj_id AS sk_dim_lojas,
        lj_nome,
        lo_cidade AS lj_lo_cidade,
        lo_estado AS lj_lo_estado,
        lo_regiao AS lj_lo_regiao
    FROM
        {{ ref('int_lojas_consolidado') }}
    ORDER BY pk_lj_id ASC
)
SELECT
    *
FROM
    source

{% endsnapshot %}