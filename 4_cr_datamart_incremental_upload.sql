WITH

-- дата последнего обновления витрины
-- считаем отдельно, чтобы в условии WHERE читалось легче
load_date AS (
	SELECT COALESCE(MAX(t.load_dttm),'1900-01-01') AS last_date
	FROM dwh.load_dates_customer_report_datamart t
),


-- топ мастеров и продуктов у заказчиков тоже посчитаем отдельно, чтобы основные запросы легче читались

-- топ мастера
top_craftsman AS (
	SELECT 
		t.customer_id AS customer_id,
		t.craftsman_id AS craftsman_id
	FROM(
		SELECT t.customer_id,
			   t.craftsman_id,
			   ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY COUNT(*) DESC) craftsman_rank
		FROM dwh.f_order t
		GROUP BY t.customer_id, t.craftsman_id
	) t
	WHERE t.craftsman_rank = 1
),

-- топ категории
top_product_type AS (
	SELECT 
		t.customer_id AS customer_id,
		t.product_type AS product_type
	FROM(
		SELECT t.customer_id,
			   p.product_type,
			   ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY COUNT(*) DESC) AS product_rank
		FROM dwh.f_order t
		INNER JOIN dwh.d_product p USING (product_id)
		GROUP BY t.customer_id, p.product_type
	) t
	WHERE t.product_rank = 1
),

-- соберем всех существующих клиентов, которые уже есть в витрине, отдельно — для удобной фильтрации в дальнейшем
old_customers AS(
	SELECT DISTINCT t.customer_id AS customer_id
	FROM dwh.customer_report_datamart t
),

-- определяем, какие данные были изменены в витрине или добавлены в DWH. Формируем полную дельту изменений
dwh_delta AS ( 
    SELECT   
		customers.customer_id AS customer_id,
		customers.customer_name AS customer_name,
		customers.customer_address AS customer_address,
		customers.customer_birthday AS customer_birthday,
		customers.customer_email AS customer_email,
		old_customers.customer_id AS exist_customer_id,
		orders.order_id AS order_id,
		products.product_id AS product_id,
		products.product_price AS product_price,
		products.product_type AS product_type,
		orders.order_completion_date - orders.order_created_date AS diff_order_date, 
		orders.order_status AS order_status,
		TO_CHAR(orders.order_created_date, 'yyyy-mm') AS report_period,
		craftsman.load_dttm AS craftsman_load_dttm,
		customers.load_dttm AS customers_load_dttm,
		products.load_dttm AS products_load_dttm
	FROM dwh.f_order orders
	INNER JOIN dwh.d_craftsman craftsman USING(craftsman_id) 
	INNER JOIN dwh.d_customer customers USING(customer_id)
	INNER JOIN dwh.d_product products USING(product_id)
	LEFT JOIN old_customers USING (customer_id)
	LEFT JOIN load_date ON 1=1
	WHERE 
		(orders.load_dttm > load_date.last_date) OR
		(craftsman.load_dttm > load_date.last_date) OR
		(customers.load_dttm > load_date.last_date) OR
		(products.load_dttm > load_date.last_date)
),

-- собираем клиентов, по которым уже были данные в витрине, но есть изменения в DWH.
-- их данные нужно будет обновить 
dwh_update_delta AS (
    SELECT DISTINCT t.exist_customer_id AS customer_id
    FROM  dwh_delta t 
    WHERE t.exist_customer_id IS NOT NULL        
),

-- считаем данные только по новым клиентам, чтобы вставить их в витрину
dwh_delta_insert_result_set AS ( 
    SELECT 
		t.customer_id AS customer_id,
		t.customer_name AS customer_name,
		t.customer_address AS customer_address,
		t.customer_birthday AS customer_birthday,
		t.customer_email AS customer_email,
		SUM(t.product_price) AS customer_money,
		SUM(t.product_price) * 0.1 AS platform_money,
		COUNT(t.order_id) AS count_order,
		AVG(t.product_price) AS avg_price_order,
		top_products.product_type AS top_product_type,
		top_craftsman.craftsman_id AS top_craftsman_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
		SUM(CASE WHEN t.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
		SUM(CASE WHEN t.order_status = 'in progress' OR t.order_status = 'in-progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
		SUM(CASE WHEN t.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
		SUM(CASE WHEN t.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
		SUM(CASE WHEN t.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
		t.report_period AS report_period
	FROM dwh_delta t
	INNER JOIN top_craftsman top_craftsman USING(customer_id)
	INNER JOIN top_product_type top_products USING(customer_id)
	WHERE t.exist_customer_id IS NULL
	GROUP BY 
		t.customer_id, 
		t.customer_name, 
		t.customer_address, 
		t.customer_birthday, 
		t.customer_email, 
		top_products.product_type,
		top_craftsman.craftsman_id,
		t.report_period
),

-- делаем перерасчёт для существующих записей витрины, так как данные обновились за отчётные периоды.
-- достаём из DWH обновлённые или новые данные по клиентам, которые уже есть в витрине
dwh_delta_update_result_set AS (
	SELECT     
		customers.customer_id AS customer_id,
		customers.customer_name AS customer_name,
		customers.customer_address AS customer_address,
		customers.customer_birthday AS customer_birthday,
		customers.customer_email AS customer_email,
		SUM(products.product_price) AS customer_money,
		SUM(products.product_price)*0.1 AS platform_money,
		COUNT(orders.order_id) AS count_order,
		AVG(products.product_price) AS avg_price_order,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY (orders.order_completion_date - orders.order_created_date)) AS median_time_order_completed,
		top_products.product_type AS top_product_type,
		top_craftsman.craftsman_id AS top_craftsman_id,
		SUM(CASE WHEN orders.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created, 
		SUM(CASE WHEN orders.order_status = 'in progress' OR orders.order_status = 'in-progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
		SUM(CASE WHEN orders.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
		SUM(CASE WHEN orders.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
		SUM(CASE WHEN orders.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
		TO_CHAR(orders.order_created_date, 'yyyy-mm') AS report_period
	FROM dwh.f_order orders 
	INNER JOIN dwh.d_craftsman craftsman USING(craftsman_id) 
	INNER JOIN dwh.d_customer customers USING(customer_id)
	INNER JOIN dwh.d_product products USING(product_id)
	INNER JOIN dwh_update_delta ud USING(customer_id)
	INNER JOIN top_craftsman top_craftsman USING(customer_id)
	INNER JOIN top_product_type top_products USING(customer_id)
	GROUP BY 
		customers.customer_id, 
		customers.customer_name, 
		customers.customer_address, 
		customers.customer_birthday, 
		customers.customer_email, 
		top_products.product_type,
		top_craftsman.craftsman_id,
		TO_CHAR(orders.order_created_date, 'yyyy-mm')
),

-- добавляем новые данные в витрину
insert_delta AS ( 
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
		customer_name,
		customer_address,
		customer_birthday,
		customer_email,
		customer_money,
		platform_money,
		count_order,
		avg_price_order,
		median_time_order_completed,
		top_product_type,
		top_craftsman_id,
		count_order_created,
		count_order_in_progress,
		count_order_delivery,
		count_order_done,
		count_order_not_done,
		report_period		
    )
    
    SELECT 		
		customer_id,
		customer_name,
		customer_address,
		customer_birthday,
		customer_email,
		customer_money,
		platform_money,
		count_order,
		avg_price_order,
		median_time_order_completed,
		top_product_type,
		top_craftsman_id,
		count_order_created,
		count_order_in_progress, 
		count_order_delivery, 
		count_order_done, 
		count_order_not_done,
		report_period
    FROM dwh_delta_insert_result_set
),

-- обновляем существующие записи в витрине
update_delta AS (
    UPDATE dwh.customer_report_datamart SET
		customer_name = updates.customer_name,
		customer_address = updates.customer_address,
		customer_birthday = updates.customer_birthday,
		customer_email = updates.customer_email,
		customer_money = updates.customer_money,
		platform_money = updates.platform_money,
		count_order = updates.count_order,
		avg_price_order = updates.avg_price_order,
		median_time_order_completed = updates.median_time_order_completed,
		top_product_type = updates.top_product_type,
		top_craftsman_id = updates.top_craftsman_id,
		count_order_created = updates.count_order_created,
		count_order_in_progress = updates.count_order_in_progress, 
		count_order_delivery = updates.count_order_delivery, 
		count_order_done = updates.count_order_done, 
		count_order_not_done = updates.count_order_not_done,
		report_period = updates.report_period
    FROM (
        SELECT 
            customer_id,
			customer_name,
			customer_address,
			customer_birthday,
			customer_email,
			customer_money,
			platform_money,
			count_order,
			avg_price_order,
			median_time_order_completed,
			top_product_type,
			top_craftsman_id,
			count_order_created,
			count_order_in_progress, 
			count_order_delivery, 
			count_order_done, 
			count_order_not_done,
			report_period
        FROM dwh_delta_update_result_set
    ) updates
    WHERE dwh.customer_report_datamart.customer_id = updates.customer_id
),

-- обновляем дату и время последнего обновления данных в витрине
insert_load_date AS (
    INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
    SELECT 
		GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                 COALESCE(MAX(customers_load_dttm), NOW()), 
                 COALESCE(MAX(products_load_dttm), NOW())) 
    FROM dwh_delta
)

SELECT 'KEK'