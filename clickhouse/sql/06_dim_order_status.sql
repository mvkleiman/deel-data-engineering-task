-- Order status lookup dimension.
-- Single source of truth for status classification used by aggregates.

CREATE TABLE IF NOT EXISTS dwh.dim_order_status (
    status        String,
    status_group  String,    -- 'open' or 'terminal'
    is_terminal   UInt8,
    display_order UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY status;

INSERT INTO dwh.dim_order_status VALUES
    ('PENDING',      'open',     0, 1),
    ('PROCESSING',   'open',     0, 2),
    ('REPROCESSING', 'open',     0, 3),
    ('COMPLETED',    'terminal', 1, 4),
    ('CANCELLED',    'terminal', 1, 5);
