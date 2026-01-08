--1
/*избыточное количество индексов на таблицу orders, можно улучшить столбцы order_dt и discount */
drop index if exists
    orders_city_id_idx,
    orders_device_type_city_id_idx,
    orders_device_type_idx,
    orders_discount_idx,
    orders_final_cost_idx,
    orders_final_cost_idx,
    orders_order_dt_idx,
    orders_total_cost_idx,
    orders_total_final_cost_discount_idx;
	
alter table orders alter column order_dt set default current_timestamp;
alter table orders alter column discount set default null;

create sequence if not exists orders_id_seq
    increment by 1
    owned by public.orders.order_id;

select setval('orders_id_seq', (select max(order_id) from orders));

alter table public.orders
alter column order_id set default nextval('orders_id_seq');

explain analyze
INSERT INTO orders
    (order_id, user_id, device_type, city_id, total_cost, final_cost)
SELECT MAX(order_id) + 1, 
    '329551a1-215d-43e6-baee-322f2467272d', 
    'Mobile', 10, 1000.00, 1000.00
FROM orders;

--2
/*неоптимальные типы данных,
последовательное сканирование таблицы */
CREATE TYPE gender_ AS ENUM     ('female','male');
--меняем типы данных
ALTER TABLE users ALTER COLUMN user_id type uuid using user_id::text::uuid;
alter table users alter column  first_name type varchar(100);
alter table users alter column  last_name type varchar(100);
alter table users alter column  city_id type int;
alter table users alter column  gender type gender_ using gender::gender_;
alter table users alter column birth_date type timestamp using to_timestamp(birth_date, 'YYYY-MM-DD HH24:MI:SS');
alter table users alter column registration_date type timestamp without time zone using to_timestamp(registration_date, 'YYYY-MM-DD HH24:MI:SS');

--создаем индекс для более эффективного поиска
create index users_city_id_idx on users(city_id);

--корректируем запрос:убираем присвоение типов для столбцов и убираем привидение к дате через to_date
SELECT user_id, first_name, last_name, 
    city_id, gender
FROM users
WHERE city_id = 4
    AND date_part('day', birth_date) = date_part('day','31-12-2023'::date)
    AND date_part('month', birth_date) = date_part('month', '31-12-2023'::date);


--3
/*неэффективная вставка в sales
таблица payments является частью таблицы orders, дублируя данные
*/
create or replace procedure add_payment(p_order_id bigint,p_sum_payment numeric)
language plpgsql
as
$$
BEGIN
    INSERT INTO order_statuses (order_id, status_id, status_dt)
    VALUES (p_order_id, 2, statement_timestamp());

    INSERT INTO sales(sale_id, sale_dt, user_id, sale_sum)
    SELECT NEXTVAL('sales_sale_id_sq'), statement_timestamp(), user_id, p_sum_payment
    FROM orders WHERE order_id = p_order_id;
END;
$$;

--удалим таблицу
drop table if exists payments;

--создадим индекс 
create index orders_order_id_user_id ON orders(order_id) include (user_id);

--4
--таблица логов большая, осуществяется поиск по 

--сделать партиционирование для таблицы внутри функции:
--удалить user_logs_pkey и создать новое ограничение user_logs_pkey на поля (log_id, log_date) 
alter table user_logs
drop constraint user_logs_pkey;

alter table user_logs
add constraint user_logs_pkey primary key (log_id, log_date);
--написать функцию, которая будет автоматом создавать квартальные партиции,если такой квартальной партиции еще нет
--написать триггер, который будет срабатывать до вставки в таблицу user_logs для каждого столбца
--посмотреть, корректно ли отрабатывает функция и триггер, вставив тестовые значения в таблицу
--проанализировать, по какой таблице идет поиск и увеличилась ли производительность (сократилось время запроса)

--5
/*отчет составляется на основе ежедневных данных, 
поэтому можно создать материализованное представление, которое можно обновлять раз в день*/
create materialized view if not exists statistics_dishes_ages_groups as(
with ages_dishes as (
select o.order_dt::date,o.user_id
,(i.spicy*oi.count)::int spicy
,(i.meat*oi.count)::int meat
,(i.fish*oi.count)::int fish
,case when extract(years from age(order_dt,birth_date))>0 and extract(years from age(order_dt,birth_date))<=20 then '0-20'
	 when extract(years from age(order_dt,birth_date))>20 and extract(years from age(order_dt,birth_date))<=30 then '20-30'
	 when extract(years from age(order_dt,birth_date))>30 and extract(years from age(order_dt,birth_date))<=40 then '30-40'
	 when extract(years from age(order_dt,birth_date))>40 and extract(years from age(order_dt,birth_date))<=100 then '40-100'
	end age
 from order_items oi
 left join orders o on oi.order_id=o.order_id
left join users u on o.user_id=u.user_id
left join dishes i on oi.item=i.object_id
where order_dt<current_date)
select order_dt,age,100*sum(spicy)/count(spicy) spicy
,100*sum(fish)/count(fish) fish
,100*sum(meat)/count(meat) meat  from ages_dishes t

group by order_dt,age
order by order_dt desc,age);
