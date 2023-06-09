{{ config(
    sort = ["_airbyte_unique_key", "_airbyte_emitted_at"],
    unique_key = "_airbyte_unique_key",
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('accruals_scd') }}
select
    _airbyte_unique_key,
    date,
    ref_id,
    created_at,
    _ab_cdc_deleted_at,
    tax,
    _ab_cdc_lsn,
    units,
    type,
    net_value,
    gross_value,
    _ab_cdc_updated_at,
    portfolio_id,
    id,
    status,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_accruals_hashid
from {{ ref('accruals_scd') }}
-- accruals from {{ source('public', '_airbyte_raw_accruals') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

