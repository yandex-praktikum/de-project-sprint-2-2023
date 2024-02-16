--BEGIN TRANSACTION;


WITH
load_date AS (-- дата последнего обновления витрины
	SELECT 
		COALESCE(MAX(t.load_dttm),'1900-01-01') AS last_date
	FROM 
		dwh.load_dates_customer_report_datamart t
),

dist_existed_customer AS(
	SELECT	
		DISTINCT t.customer_id AS customer_id
	FROM dwh.customer_report_datamart t
),

top_craftsman AS (-- самый популярный мастер у клиента (покупателя) в заказах
	SELECT 
		t.customer_id AS customer_id,
		t.craftsman_id AS craftsman_id
	FROM
		(SELECT
			t.customer_id,
			t.craftsman_id,
			ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY COUNT(*) DESC) row_rank
		FROM 
			dwh.f_order t
		GROUP BY
			t.customer_id,
			t.craftsman_id
		) t
	WHERE t.row_rank = 1
),

top_product_type AS (-- самая популярная категория товаров у клиента в заказах
	SELECT 
		t.customer_id AS customer_id,
		t.product_type AS product_type
	FROM
		(SELECT
			t.customer_id,
			p.product_type,
			ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY COUNT(*) DESC) AS row_rank
		FROM 
			dwh.f_order t
			INNER join dwh.d_product p USING (product_id)
		GROUP BY
			t.customer_id,
			p.product_type
		) t
	WHERE t.row_rank = 1
),

dwh_total_delta AS ( -- определяем, какие данные были изменены в витрине или добавлены в DWH. Формируем полную дельту изменений
    SELECT   
		cust.customer_id AS customer_id,
		cust.customer_name AS customer_name,
		cust.customer_address AS customer_address,
		cust.customer_birthday AS customer_birthday,
		cust.customer_email AS customer_email,
		excust.customer_id AS exist_customer_id,
		orders.order_id AS order_id,
		prod.product_id AS product_id,
		prod.product_price AS product_price,
		prod.product_type AS product_type,
		orders.order_completion_date - orders.order_created_date AS diff_order_date, 
		orders.order_status AS order_status,
		TO_CHAR(orders.order_created_date, 'yyyy-mm') AS report_period,
		craft.load_dttm AS craftsman_load_dttm,
		cust.load_dttm AS customers_load_dttm,
		prod.load_dttm AS products_load_dttm
	FROM dwh.f_order orders
		INNER JOIN dwh.d_craftsman craft USING(craftsman_id)--уточнить у бизнеса, требуется ли учитывать изменения перс.данных мастера, возможно, они не имеют значения, тогда джойн и условие в блоке where можно убрать
		INNER JOIN dwh.d_customer cust USING(customer_id)
		INNER JOIN dwh.d_product prod USING(product_id)
		LEFT JOIN dist_existed_customer excust USING (customer_id)
		LEFT JOIN load_date ON 1=1
	WHERE 
		(orders.load_dttm > load_date.last_date) OR
		(craft.load_dttm > load_date.last_date) OR
		(cust.load_dttm > load_date.last_date) OR
		(prod.load_dttm > load_date.last_date)
),

dwh_update_delta AS ( -- делаем выборку клиентов, по которым были данные уже в витрине, но есть изменения в DWH. По этим клиентам данные в витрине нужно будет обновить
    SELECT     
        DISTINCT t.exist_customer_id AS customer_id
    FROM 
		dwh_total_delta t 
    WHERE 
		t.exist_customer_id IS NOT NULL        
),

dwh_delta_insert_result_set AS ( -- делаем расчёт витрины по новым клиентам. Их можно просто вставить (insert) в витрину без обновления
    SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
		t.customer_id AS customer_id,
		t.customer_name AS customer_name,
		t.customer_address AS customer_address,
		t.customer_birthday AS customer_birthday,
		t.customer_email AS customer_email,
		SUM(t.product_price) AS customer_money,
		SUM(t.product_price) * 0.1 AS platform_money,
		COUNT(t.order_id) AS count_order,
		AVG(t.product_price) AS avg_price_order,
		top_prod.product_type AS top_product_type,
		top_craft.craftsman_id AS top_craftsman_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
		SUM(CASE WHEN t.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
		SUM(CASE WHEN t.order_status = 'in progress' OR t.order_status = 'in-progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
		SUM(CASE WHEN t.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
		SUM(CASE WHEN t.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
		SUM(CASE WHEN t.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
		t.report_period AS report_period
	FROM dwh_total_delta t
	INNER JOIN top_craftsman top_craft USING(customer_id)
	INNER JOIN top_product_type top_prod USING(customer_id)
	WHERE 
		t.exist_customer_id IS NULL
	GROUP BY 
		t.customer_id, 
		t.customer_name, 
		t.customer_address, 
		t.customer_birthday, 
		t.customer_email, 
		top_prod.product_type,
		top_craft.craftsman_id,
		t.report_period
),

dwh_delta_update_result_set AS ( -- делаем перерасчёт для существующих записей витрины, так как данные обновились за отчётные периоды. Логика похожа на insert, но нужно достать конкретные данные из DWH
	SELECT     -- в этой выборке достаём из DWH обновлённые или новые данные по клиентам, которые уже есть в витрине
		cust.customer_id AS customer_id,
		cust.customer_name AS customer_name,
		cust.customer_address AS customer_address,
		cust.customer_birthday AS customer_birthday,
		cust.customer_email AS customer_email,
		SUM(prod.product_price) AS customer_money,
		SUM(prod.product_price)*0.1 AS platform_money,
		COUNT(orders.order_id) AS count_order,
		AVG(prod.product_price) AS avg_price_order,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY (orders.order_completion_date - orders.order_created_date)) AS median_time_order_completed,
		top_prod.product_type AS top_product_type,
		top_craft.craftsman_id AS top_craftsman_id,
		SUM(CASE WHEN orders.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created, 
		SUM(CASE WHEN orders.order_status = 'in progress' OR orders.order_status = 'in-progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
		SUM(CASE WHEN orders.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
		SUM(CASE WHEN orders.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
		SUM(CASE WHEN orders.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
		TO_CHAR(orders.order_created_date, 'yyyy-mm') AS report_period
	FROM dwh.f_order orders 
	INNER JOIN dwh.d_craftsman craft USING(craftsman_id) 
	INNER JOIN dwh.d_customer cust USING(customer_id)
	INNER JOIN dwh.d_product prod USING(product_id)
	INNER JOIN dwh_update_delta ud USING(customer_id)
	INNER JOIN top_craftsman top_craft USING(customer_id)
	INNER JOIN top_product_type top_prod USING(customer_id)
	GROUP BY 
		cust.customer_id, 
		cust.customer_name, 
		cust.customer_address, 
		cust.customer_birthday, 
		cust.customer_email, 
		top_prod.product_type,
		top_craft.craftsman_id,
		TO_CHAR(orders.order_created_date, 'yyyy-mm')
),

insert_delta AS ( -- выполняем insert новых расчитанных данных для витрины 
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
    ) SELECT 		
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

update_delta AS ( -- выполняем обновление показателей в отчёте по уже существующим мастерам
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
        FROM dwh_delta_update_result_set) updates
    WHERE dwh.customer_report_datamart.customer_id = updates.customer_id
),

insert_load_date AS ( -- делаем запись в таблицу загрузок о том, когда была совершена загрузка, чтобы в следующий раз взять данные, которые будут добавлены или изменены после этой даты
    INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
    SELECT 
		GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                 COALESCE(MAX(customers_load_dttm), NOW()), 
                 COALESCE(MAX(products_load_dttm), NOW())) 
    FROM dwh_total_delta
)

SELECT 'Launching incremental uploading dwh.cusomer_report_datamart '||now();



--COMMIT TRANSACTION;