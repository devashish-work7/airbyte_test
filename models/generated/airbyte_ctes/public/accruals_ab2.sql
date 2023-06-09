{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('accruals_ab1') }}
select
    cast({{ empty_string_to_null('date') }} as {{ type_date() }}) as date,
    cast(ref_id as {{ dbt_utils.type_bigint() }}) as ref_id,
    cast({{ empty_string_to_null('created_at') }} as {{ type_timestamp_without_timezone() }}) as created_at,
    cast(_ab_cdc_deleted_at as {{ dbt_utils.type_string() }}) as _ab_cdc_deleted_at,
    cast(tax as {{ dbt_utils.type_float() }}) as tax,
    cast(_ab_cdc_lsn as {{ dbt_utils.type_float() }}) as _ab_cdc_lsn,
    cast(units as {{ dbt_utils.type_float() }}) as units,
    cast(type as {{ dbt_utils.type_string() }}) as type,
    cast(net_value as {{ dbt_utils.type_float() }}) as net_value,
    cast(gross_value as {{ dbt_utils.type_float() }}) as gross_value,
    cast(_ab_cdc_updated_at as {{ dbt_utils.type_string() }}) as _ab_cdc_updated_at,
    cast(portfolio_id as {{ dbt_utils.type_bigint() }}) as portfolio_id,
    cast(id as {{ dbt_utils.type_bigint() }}) as id,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('accruals_ab1') }}
-- accruals
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

