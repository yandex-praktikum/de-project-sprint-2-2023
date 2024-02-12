-- DDL витрины данных с отчётом по заказчикам
DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
    -------------------------------------------------
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,--1 идентификатор записи;
	customer_id BIGINT NOT NULL,--2 идентификатор заказчика;
	customer_name VARCHAR NOT NULL,--3 Ф. И. О. заказчика;
	customer_address VARCHAR NOT NULL,--4 адрес заказчика;
	customer_birthday DATE NOT NULL,--5 дата рождения заказчика;
	customer_email VARCHAR NOT NULL,--6 электронная почта заказчика;
	customer_money NUMERIC(15,2) NOT NULL,--7 сумма, которую потратил заказчик;
	platform_money NUMERIC(15,2) NOT NULL,--8 сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик);
	count_order BIGINT NOT NULL,--9 количество заказов у заказчика за месяц;
	avg_price_order NUMERIC(10,2) NOT NULL,--10 средняя стоимость одного заказа у заказчика за месяц;
	median_time_order_completed NUMERIC(10,1),--11 медианное время в днях от момента создания заказа до его завершения за месяц;
	top_product_category VARCHAR NOT NULL, --12 самая популярная категория товаров у этого заказчика за месяц;
	top_craftsman BIGINT NOT NULL,--13 идентификатор самого популярного мастера ручной работы у заказчика. Если заказчик сделал одинаковое количество заказов у нескольких мастеров, возьмите любого;
	count_order_created BIGINT NOT NULL,--14 количество созданных заказов за месяц;
	count_order_in_progress BIGINT NOT NULL,--15 количество заказов в процессе изготовки за месяц;
	count_order_delivery BIGINT NOT NULL,--16 количество заказов в доставке за месяц;
	count_order_done BIGINT NOT NULL,--17 количество завершённых заказов за месяц;
	count_order_not_done BIGINT NOT NULL,--18 количество незавершённых заказов за месяц;
	report_period VARCHAR NOT NULL,--19 отчётный период, год и месяц.
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);

-- DDL таблицы инкрементальных загрузок с отчётом по заказчикам
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);