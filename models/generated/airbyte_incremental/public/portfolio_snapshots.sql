{{ config(
    sort = ["_airbyte_unique_key", "_airbyte_emitted_at"],
    unique_key = "_airbyte_unique_key",
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('portfolio_snapshots_scd') }}
select
    _airbyte_unique_key,
    date,
    risk_target,
    pnl_mtd,
    rebalanced,
    holdings_value,
    created_at,
    _ab_cdc_deleted_at,
    _ab_cdc_lsn,
    risk_actual,
    settled_non_dividend_accrual,
    pnl_affecting_transactions_sum,
    balance,
    holdings_cost,
    pnl_ytd,
    meta,
    pnl_daily,
    pnl_inception,
    _ab_cdc_updated_at,
    portfolio_id,
    accrual,
    holdings_profit,
    status,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_portfolio_snapshots_hashid
from {{ ref('portfolio_snapshots_scd') }}
-- portfolio_snapshots from {{ source('public', '_airbyte_raw_portfolio_snapshots') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

