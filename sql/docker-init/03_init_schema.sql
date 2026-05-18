create schema if not exists snowflake;

-- таблица стран
create table if not exists snowflake.dim_country (
    country_id serial primary key,
    country_name varchar(50) unique
);

-- таблица покупателей
create table if not exists snowflake.dim_buyer (
    buyer_id serial primary key,
    first_name varchar(50),
    last_name varchar(50),
    age int,
    email varchar(50),
    country_id int references snowflake.dim_country(country_id),
    postal_code varchar(50)
);

-- таблица животных
create table if not exists snowflake.dim_animal (
    animal_id serial primary key,
    animal_name varchar(50),
    animal_type varchar(50),
    animal_breed varchar(50),
    animal_category varchar(50),
    unique (animal_name, animal_type)
);

-- таблица сотрудников
create table if not exists snowflake.dim_employee (
    employee_id serial primary key,
    first_name varchar(50),
    last_name varchar(50),
    email varchar(50),
    country_id int references snowflake.dim_country(country_id),
    postal_code varchar(50)
);

-- таблица торговых точек
create table if not exists snowflake.dim_shop (
    shop_id serial primary key,
    shop_name varchar(50),
    address varchar(50),
    city varchar(50),
    region varchar(50),
    country_id int references snowflake.dim_country(country_id),
    phone varchar(50),
    email varchar(50),
    unique (shop_name, city)
);

-- таблица производителей (одна строка на имя поставщика)
create table if not exists snowflake.dim_vendor (
    vendor_id serial primary key,
    vendor_name varchar(50) unique,
    contact_person varchar(50),
    email varchar(50),
    phone varchar(50),
    address varchar(50),
    city varchar(50),
    country_id int references snowflake.dim_country(country_id)
);

-- таблица товаров
create table if not exists snowflake.dim_item (
    item_id serial primary key,
    item_name varchar(50),
    category varchar(50),
    price numeric(10,2),
    weight numeric(10,2),
    color varchar(50),
    size varchar(50),
    brand varchar(50),
    material varchar(50),
    description text,
    rating numeric(3,2),
    reviews_count int,
    release_date date,
    expiry_date date,
    vendor_id int references snowflake.dim_vendor(vendor_id)
);

-- таблица продаж
create table if not exists snowflake.fact_transactions (
    transaction_id serial primary key,
    buyer_id int references snowflake.dim_buyer(buyer_id),
    animal_id int references snowflake.dim_animal(animal_id),
    employee_id int references snowflake.dim_employee(employee_id),
    shop_id int references snowflake.dim_shop(shop_id),
    item_id int references snowflake.dim_item(item_id),
    sale_date timestamp,
    quantity int,
    total_amount numeric(10,2)
);
