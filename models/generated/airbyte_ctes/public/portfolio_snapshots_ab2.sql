{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('portfolio_snapshots_ab1') }}
select
    cast({{ empty_string_to_null('date') }} as {{ type_date() }}) as date,
    cast(risk_target as {{ dbt_utils.type_float() }}) as risk_target,
    cast(pnl_mtd as {{ dbt_utils.type_float() }}) as pnl_mtd,
    {{ cast_to_boolean('rebalanced') }} as rebalanced,
    cast(holdings_value as {{ dbt_utils.type_float() }}) as holdings_value,
    cast({{ empty_string_to_null('created_at') }} as {{ type_timestamp_without_timezone() }}) as created_at,
    cast(_ab_cdc_deleted_at as {{ dbt_utils.type_string() }}) as _ab_cdc_deleted_at,
    cast(_ab_cdc_lsn as {{ dbt_utils.type_float() }}) as _ab_cdc_lsn,
    cast(risk_actual as {{ dbt_utils.type_float() }}) as risk_actual,
    cast(settled_non_dividend_accrual as {{ dbt_utils.type_float() }}) as settled_non_dividend_accrual,
    cast(pnl_affecting_transactions_sum as {{ dbt_utils.type_float() }}) as pnl_affecting_transactions_sum,
    cast(balance as {{ dbt_utils.type_float() }}) as balance,
    cast(holdings_cost as {{ dbt_utils.type_float() }}) as holdings_cost,
    cast(pnl_ytd as {{ dbt_utils.type_float() }}) as pnl_ytd,
    cast(meta as {{ dbt_utils.type_string() }}) as meta,
    cast(pnl_daily as {{ dbt_utils.type_float() }}) as pnl_daily,
    cast(pnl_inception as {{ dbt_utils.type_float() }}) as pnl_inception,
    cast(_ab_cdc_updated_at as {{ dbt_utils.type_string() }}) as _ab_cdc_updated_at,
    cast(portfolio_id as {{ dbt_utils.type_bigint() }}) as portfolio_id,
    cast(accrual as {{ dbt_utils.type_float() }}) as accrual,
    cast(holdings_profit as {{ dbt_utils.type_float() }}) as holdings_profit,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('portfolio_snapshots_ab1') }}
-- portfolio_snapshots
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

