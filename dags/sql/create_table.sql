-- creates the table to snowflake

CREATE TABLE IF NOT EXISTS weather_table (
    data_id int IDENTITY(1, 1),
    latitude FLOAT,
    longitude FLOAT,
    temperature FLOAT,
    rh FLOAT
);