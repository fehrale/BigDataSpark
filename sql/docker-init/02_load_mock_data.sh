#!/bin/bash
set -euo pipefail
export PGPASSWORD="${POSTGRES_PASSWORD}"
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<'EOSQL'
COPY public.mock_data FROM '/csv/MOCK_DATA.csv' WITH (FORMAT csv, HEADER true);
COPY public.mock_data FROM '/csv/MOCK_DATA (1).csv' WITH (FORMAT csv, HEADER true);
COPY public.mock_data FROM '/csv/MOCK_DATA (2).csv' WITH (FORMAT csv, HEADER true);
COPY public.mock_data FROM '/csv/MOCK_DATA (3).csv' WITH (FORMAT csv, HEADER true);
COPY public.mock_data FROM '/csv/MOCK_DATA (4).csv' WITH (FORMAT csv, HEADER true);
COPY public.mock_data FROM '/csv/MOCK_DATA (5).csv' WITH (FORMAT csv, HEADER true);
COPY public.mock_data FROM '/csv/MOCK_DATA (6).csv' WITH (FORMAT csv, HEADER true);
COPY public.mock_data FROM '/csv/MOCK_DATA (7).csv' WITH (FORMAT csv, HEADER true);
COPY public.mock_data FROM '/csv/MOCK_DATA (8).csv' WITH (FORMAT csv, HEADER true);
COPY public.mock_data FROM '/csv/MOCK_DATA (9).csv' WITH (FORMAT csv, HEADER true);
EOSQL
