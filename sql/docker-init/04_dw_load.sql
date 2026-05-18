insert into snowflake.dim_country (country_name)
select distinct country_name from (
    select customer_country as country_name from public.mock_data
    union
    select seller_country from public.mock_data
    union
    select store_country from public.mock_data
    union
    select supplier_country from public.mock_data
) t
where country_name is not null and trim(country_name) <> '';

-- покупатели
insert into snowflake.dim_buyer (first_name, last_name, age, email, country_id, postal_code)
select distinct
    m.customer_first_name,
    m.customer_last_name,
    m.customer_age,
    m.customer_email,
    c.country_id,
    m.customer_postal_code
from public.mock_data m
left join snowflake.dim_country c on c.country_name = m.customer_country
where m.customer_first_name is not null;

-- животные
insert into snowflake.dim_animal (animal_name, animal_type, animal_breed, animal_category)
select distinct on (customer_pet_name, customer_pet_type)
    customer_pet_name,
    customer_pet_type,
    customer_pet_breed,
    pet_category
from public.mock_data
where trim(coalesce(customer_pet_name,'')) <> ''
order by customer_pet_name, customer_pet_type, pet_category desc nulls last;

-- сотрудники
insert into snowflake.dim_employee (first_name, last_name, email, country_id, postal_code)
select distinct
    m.seller_first_name,
    m.seller_last_name,
    m.seller_email,
    c.country_id,
    m.seller_postal_code
from public.mock_data m
left join snowflake.dim_country c on c.country_name = m.seller_country
where m.seller_first_name is not null;

-- торговые точки
insert into snowflake.dim_shop (shop_name, address, city, region, country_id, phone, email)
select distinct on (m.store_name, m.store_city)
    m.store_name,
    m.store_location,
    m.store_city,
    m.store_state,
    c.country_id,
    m.store_phone,
    m.store_email
from public.mock_data m
left join snowflake.dim_country c on c.country_name = m.store_country
where trim(coalesce(m.store_name,'')) <> ''
order by m.store_name, m.store_city, m.store_state nulls last;

-- производители (одно измерение на supplier_name — иначе join раздувает dim_item)
insert into snowflake.dim_vendor (vendor_name, contact_person, email, phone, address, city, country_id)
select distinct on (m.supplier_name)
    m.supplier_name,
    m.supplier_contact,
    m.supplier_email,
    m.supplier_phone,
    m.supplier_address,
    m.supplier_city,
    c.country_id
from public.mock_data m
left join snowflake.dim_country c on c.country_name = m.supplier_country
where trim(coalesce(m.supplier_name,'')) <> ''
order by m.supplier_name, m.supplier_email;

-- товары (одна строка на товар+category+vendor — иначе join к факту размножает строки)
insert into snowflake.dim_item (
    item_name, category, price, weight, color, size, brand, material,
    description, rating, reviews_count, release_date, expiry_date, vendor_id
)
select distinct on (m.product_name, m.product_category, v.vendor_id)
    m.product_name,
    m.product_category,
    m.product_price,
    m.product_weight,
    m.product_color,
    m.product_size,
    m.product_brand,
    m.product_material,
    m.product_description,
    m.product_rating,
    m.product_reviews,
    case when trim(coalesce(m.product_release_date,'')) <> ''
         then to_date(trim(m.product_release_date), 'MM/DD/YYYY') end,
    case when trim(coalesce(m.product_expiry_date,'')) <> ''
         then to_date(trim(m.product_expiry_date), 'MM/DD/YYYY') end,
    v.vendor_id
from public.mock_data m
left join snowflake.dim_vendor v on v.vendor_name = m.supplier_name
where trim(coalesce(m.product_name,'')) <> ''
order by m.product_name, m.product_category, v.vendor_id, m.product_rating desc nulls last;

-- транзакции
insert into snowflake.fact_transactions (
    buyer_id, animal_id, employee_id, shop_id, item_id, sale_date,
    quantity, total_amount
)
select
    b.buyer_id,
    a.animal_id,
    e.employee_id,
    s.shop_id,
    i.item_id,
    to_timestamp(trim(coalesce(m.sale_date,'')), 'MM/DD/YYYY'),
    m.sale_quantity,
    m.sale_total_price
from public.mock_data m
left join snowflake.dim_buyer b on b.first_name = m.customer_first_name
    and b.last_name = m.customer_last_name
    and b.email = m.customer_email
left join snowflake.dim_animal a on a.animal_name = m.customer_pet_name
    and a.animal_type = m.customer_pet_type
left join snowflake.dim_employee e on e.first_name = m.seller_first_name
    and e.last_name = m.seller_last_name
    and e.email = m.seller_email
left join snowflake.dim_shop s on s.shop_name = m.store_name
    and s.city = m.store_city
left join snowflake.dim_vendor v on v.vendor_name = m.supplier_name
left join snowflake.dim_item i on i.item_name = m.product_name
    and i.category = m.product_category
    and (i.vendor_id = v.vendor_id or (v.vendor_id is null and i.vendor_id is null))
where m.sale_quantity is not null;
