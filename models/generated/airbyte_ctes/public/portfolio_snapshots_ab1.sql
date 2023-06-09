{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_portfolio_snapshots') }}
select
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as date,
    {{ json_extract_scalar('_airbyte_data', ['risk_target'], ['risk_target']) }} as risk_target,
    {{ json_extract_scalar('_airbyte_data', ['pnl_mtd'], ['pnl_mtd']) }} as pnl_mtd,
    {{ json_extract_scalar('_airbyte_data', ['rebalanced'], ['rebalanced']) }} as rebalanced,
    {{ json_extract_scalar('_airbyte_data', ['holdings_value'], ['holdings_value']) }} as holdings_value,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['_ab_cdc_deleted_at'], ['_ab_cdc_deleted_at']) }} as _ab_cdc_deleted_at,
    {{ json_extract_scalar('_airbyte_data', ['_ab_cdc_lsn'], ['_ab_cdc_lsn']) }} as _ab_cdc_lsn,
    {{ json_extract_scalar('_airbyte_data', ['risk_actual'], ['risk_actual']) }} as risk_actual,
    {{ json_extract_scalar('_airbyte_data', ['settled_non_dividend_accrual'], ['settled_non_dividend_accrual']) }} as settled_non_dividend_accrual,
    {{ json_extract_scalar('_airbyte_data', ['pnl_affecting_transactions_sum'], ['pnl_affecting_transactions_sum']) }} as pnl_affecting_transactions_sum,
    {{ json_extract_scalar('_airbyte_data', ['balance'], ['balance']) }} as balance,
    {{ json_extract_scalar('_airbyte_data', ['holdings_cost'], ['holdings_cost']) }} as holdings_cost,
    {{ json_extract_scalar('_airbyte_data', ['pnl_ytd'], ['pnl_ytd']) }} as pnl_ytd,
    {{ json_extract_scalar('_airbyte_data', ['meta'], ['meta']) }} as meta,
    {{ json_extract_scalar('_airbyte_data', ['pnl_daily'], ['pnl_daily']) }} as pnl_daily,
    {{ json_extract_scalar('_airbyte_data', ['pnl_inception'], ['pnl_inception']) }} as pnl_inception,
    {{ json_extract_scalar('_airbyte_data', ['_ab_cdc_updated_at'], ['_ab_cdc_updated_at']) }} as _ab_cdc_updated_at,
    {{ json_extract_scalar('_airbyte_data', ['portfolio_id'], ['portfolio_id']) }} as portfolio_id,
    {{ json_extract_scalar('_airbyte_data', ['accrual'], ['accrual']) }} as accrual,
    {{ json_extract_scalar('_airbyte_data', ['holdings_profit'], ['holdings_profit']) }} as holdings_profit,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_portfolio_snapshots') }} as table_alias
-- portfolio_snapshots
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

