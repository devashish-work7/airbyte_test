{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('portfolio_snapshots_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'date',
        'risk_target',
        'pnl_mtd',
        boolean_to_string('rebalanced'),
        'holdings_value',
        'created_at',
        '_ab_cdc_deleted_at',
        '_ab_cdc_lsn',
        'risk_actual',
        'settled_non_dividend_accrual',
        'pnl_affecting_transactions_sum',
        'balance',
        'holdings_cost',
        'pnl_ytd',
        'meta',
        'pnl_daily',
        'pnl_inception',
        '_ab_cdc_updated_at',
        'portfolio_id',
        'accrual',
        'holdings_profit',
        'status',
    ]) }} as _airbyte_portfolio_snapshots_hashid,
    tmp.*
from {{ ref('portfolio_snapshots_ab2') }} tmp
-- portfolio_snapshots
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

