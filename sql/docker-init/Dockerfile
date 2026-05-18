FROM postgres:16

WORKDIR /docker-entrypoint-initdb.d
COPY 01_mock_data.sql 02_load_mock_data.sh 03_init_schema.sql 04_dw_load.sql ./
RUN sed -i 's/\r$//' 02_load_mock_data.sh && chmod +x 02_load_mock_data.sh
