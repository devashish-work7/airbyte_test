{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('accruals_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'date',
        'ref_id',
        'created_at',
        '_ab_cdc_deleted_at',
        'tax',
        '_ab_cdc_lsn',
        'units',
        'type',
        'net_value',
        'gross_value',
        '_ab_cdc_updated_at',
        'portfolio_id',
        'id',
        'status',
    ]) }} as _airbyte_accruals_hashid,
    tmp.*
from {{ ref('accruals_ab2') }} tmp
-- accruals
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

