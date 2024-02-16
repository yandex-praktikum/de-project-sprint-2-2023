/* создание таблицы tmp_sources с данными из всех источников */
DROP TABLE IF EXISTS tmp_sources;
CREATE TEMP TABLE tmp_sources AS 
SELECT
	'source1' source_system,
	order_id,
    order_created_date,
    order_completion_date,
    order_status,
    craftsman_id,
    craftsman_name,
    craftsman_address,
    craftsman_birthday,
    craftsman_email,
    product_id,
    product_name,
    product_description,
    product_type,
    product_price,
    customer_id,
    customer_name,
    customer_address,
    customer_birthday,
    customer_email 
FROM source1.craft_market_wide
UNION
SELECT 
	'source2' source_system,
	t2.order_id,
    t2.order_created_date,
    t2.order_completion_date,
    t2.order_status,
    t1.craftsman_id,
    t1.craftsman_name,
    t1.craftsman_address,
    t1.craftsman_birthday,
    t1.craftsman_email,
    t1.product_id,
    t1.product_name,
    t1.product_description,
    t1.product_type,
    t1.product_price,
    t2.customer_id,
    t2.customer_name,
    t2.customer_address,
    t2.customer_birthday,
    t2.customer_email 
FROM source2.craft_market_masters_products t1 
     JOIN source2.craft_market_orders_customers t2 
     	ON t2.product_id = t1.product_id 
     	AND t1.craftsman_id = t2.craftsman_id 
UNION
SELECT 
	'source3' source_system,
	t1.order_id,
    t1.order_created_date,
    t1.order_completion_date,
    t1.order_status,
    t2.craftsman_id,
    t2.craftsman_name,
    t2.craftsman_address,
    t2.craftsman_birthday,
    t2.craftsman_email,
    t1.product_id,
    t1.product_name,
    t1.product_description,
    t1.product_type,
    t1.product_price,
    t3.customer_id,
    t3.customer_name,
    t3.customer_address,
    t3.customer_birthday,
    t3.customer_email
FROM source3.craft_market_orders t1
     JOIN source3.craft_market_craftsmans t2 
     	ON t1.craftsman_id = t2.craftsman_id 
     JOIN source3.craft_market_customers t3 
     	ON t1.customer_id = t3.customer_id
UNION     
SELECT  
	'enternal_source' source_system,
	t1.order_id,
    t1.order_created_date,
    t1.order_completion_date,
    t1.order_status,
    t1.craftsman_id,
    t1.craftsman_name,
    t1.craftsman_address,
    t1.craftsman_birthday,
    t1.craftsman_email,
    t1.product_id,
    t1.product_name,
    t1.product_description,
    t1.product_type,
    t1.product_price,
    t1.customer_id,
    t2.customer_name,
    t2.customer_address,
    t2.customer_birthday,
    t2.customer_email 
FROM 
	external_source.craft_products_orders t1
	JOIN external_source.customers t2 
		ON t1.customer_id = t2.customer_id;


/* обновление существующих записей и добавление новых в dwh.d_craftsmans */
MERGE INTO dwh.d_craftsman d
USING (SELECT DISTINCT craftsman_name, craftsman_address, craftsman_birthday, craftsman_email FROM tmp_sources) t
	ON d.craftsman_name = t.craftsman_name 
	AND d.craftsman_email = t.craftsman_email
WHEN MATCHED THEN
  UPDATE SET 
	craftsman_address = t.craftsman_address, 
	craftsman_birthday = t.craftsman_birthday, 
	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
  VALUES (t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);

/* обновление существующих записей и добавление новых в dwh.d_products */
MERGE INTO dwh.d_product d
USING (SELECT DISTINCT product_name, product_description, product_type, product_price FROM tmp_sources) t
	ON d.product_name = t.product_name 
	AND d.product_description = t.product_description 
	AND d.product_price = t.product_price
WHEN MATCHED THEN
  UPDATE SET 
	product_type = t.product_type, 
	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_name, product_description, product_type, product_price, load_dttm)
  VALUES (t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);

/* обновление существующих записей и добавление новых в dwh.d_customer */
MERGE INTO dwh.d_customer d
USING (SELECT DISTINCT customer_name, customer_address, customer_birthday, customer_email FROM tmp_sources) t
	ON d.customer_name = t.customer_name 
	AND d.customer_email = t.customer_email
WHEN MATCHED THEN
  UPDATE SET 
	customer_address= t.customer_address, 
	customer_birthday = t.customer_birthday, 
	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
  VALUES (t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);

/* создание таблицы tmp_sources_fact */
DROP TABLE IF EXISTS tmp_sources_fact;
CREATE TEMP TABLE tmp_sources_fact AS 
SELECT  
	dp.product_id,
	dc.craftsman_id,
	dcust.customer_id,
	src.order_created_date,
	src.order_completion_date,
	src.order_status,
	current_timestamp 
FROM 
	tmp_sources src
	JOIN dwh.d_craftsman dc 
		ON dc.craftsman_name = src.craftsman_name 
		AND dc.craftsman_email = src.craftsman_email 
	JOIN dwh.d_customer dcust 
		ON dcust.customer_name = src.customer_name 
		AND dcust.customer_email = src.customer_email 
	JOIN dwh.d_product dp 
		ON dp.product_name = src.product_name 
		AND dp.product_description = src.product_description 
		AND dp.product_price = src.product_price;

/* обновление существующих записей и добавление новых в dwh.f_order */
MERGE INTO dwh.f_order f
USING tmp_sources_fact t
	ON f.product_id = t.product_id 
	AND f.craftsman_id = t.craftsman_id 
	AND f.customer_id = t.customer_id 
	AND f.order_created_date = t.order_created_date 
WHEN MATCHED THEN
  UPDATE SET 
	order_completion_date = t.order_completion_date, 
	order_status = t.order_status, 
	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
  VALUES (t.product_id, t.craftsman_id, t.customer_id, t.order_created_date, t.order_completion_date, t.order_status, current_timestamp);