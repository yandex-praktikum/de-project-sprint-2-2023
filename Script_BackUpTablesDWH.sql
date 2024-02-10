-- Проверка количества полей в таблицах
select 'd_craftsman' as tabName,count(t.*) as countRows from dwh.d_craftsman t --group by tabName
union
select 'd_customer' as tabName,count(t.*) as countRows from dwh.d_customer t --group by tabName
union
select 'd_product' as tabName,count(t.*) as countRows from dwh.d_product t --group by tabName
union
select 'f_order' as tabName,count(t.*) as countRows from dwh.f_order t --group by tabName

-- Создание Резервной копии таблиц c данными схемы DWH
create schema backup; 
create table backup.d_craftsman as select * from dwh.d_craftsman;
create table backup.d_customer as select * from dwh.d_customer;
create table backup.d_product as select * from dwh.d_product;
create table backup.f_order as select * from dwh.f_order;

-- Восстановление данных из Резервной копии
delete from dwh.f_order;
delete from dwh.d_craftsman;
delete from dwh.d_customer;
delete from dwh.d_product;
INSERT INTO dwh.d_craftsman
(craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
OVERRIDING SYSTEM VALUE
SELECT craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm
FROM backup.d_craftsman;
INSERT INTO dwh.d_customer
(customer_id, customer_name, customer_address, customer_birthday, customer_email, load_dttm)
OVERRIDING SYSTEM VALUE
SELECT customer_id, customer_name, customer_address, customer_birthday, customer_email, load_dttm
FROM backup.d_customer;
INSERT INTO dwh.d_product
(product_id, product_name, product_description, product_type, product_price, load_dttm)
OVERRIDING SYSTEM VALUE
SELECT product_id, product_name, product_description, product_type, product_price, load_dttm
FROM backup.d_product;
INSERT INTO dwh.f_order
(order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
OVERRIDING SYSTEM VALUE
SELECT order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm
FROM backup.f_order;

-- Проверка количества полей в таблицах ререзвной копии 
with DWH_BKP as (
select A.tabName,A.countRows from 
(select 'd_craftsman' as tabName,count(t.*) as countRows from backup.d_craftsman t --group by tabName
union
select 'd_customer' as tabName,count(t.*) as countRows from backup.d_customer t --group by tabName
union
select 'd_product' as tabName,count(t.*) as countRows from backup.d_product t --group by tabName
union
select 'f_order' as tabName,count(t.*) as countRows from backup.f_order t) A), --group by tabName
DWH_CNT as (
select A.tabName,A.countRows from 
(select 'd_craftsman' as tabName,count(t.*) as countRows from dwh.d_craftsman t --group by tabName
union
select 'd_customer' as tabName,count(t.*) as countRows from dwh.d_customer t --group by tabName
union
select 'd_product' as tabName,count(t.*) as countRows from dwh.d_product t --group by tabName
union
select 'f_order' as tabName,count(t.*) as countRows from dwh.f_order t) A) --group by tabName
select A.*,B.countRows as countRBack from DWH_BKP A
left join DWH_CNT B on A.tabName=B.tabName; 




