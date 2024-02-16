DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY (MINVALUE 0 NO MAXVALUE START 0 NO CYCLE),
	customer_id INT8 NOT NULL,
	customer_name varchar,	
	customer_address varchar,
	customer_birthday date,
	customer_email varchar NOT NULL,
	customer_money numeric(15,2) NOT NULL,	
	platform_money numeric(15,2) NOT NULL,
	count_order int8 NOT NULL,
	avg_price_order numeric(10, 2) NOT NULL,
	median_time_order_completed numeric(10, 1) NULL,
	top_product_type varchar NOT NULL,
	top_craftsman_id int8 NOT NULL,
	count_order_created int8 NOT NULL,
	count_order_in_progress int8 NOT NULL,
	count_order_delivery int8 NOT NULL,
	count_order_done int8 NOT NULL,
	count_order_not_done int8 NOT NULL,
	report_period varchar NOT NULL,
	CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);



