--смотрим id базы, в которой работаем
select oid, datname from pg_database;

--находим 5 самых долгих запросов
select
    query,
    ROUND(mean_exec_time::numeric,2),
    ROUND(total_exec_time::numeric,2),
    ROUND(min_exec_time::numeric,2),
    ROUND(max_exec_time::numeric,2),
    calls,
    rows
from pg_stat_statements
where dbid = 50317 
order by mean_exec_time desc
limit 5;

--9,8,7,2,15

------- 9
-- определяет количество неоплаченных заказов
--  медленный nested loop, неэффективный запрос
explain analyze
SELECT count(*)
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
WHERE (SELECT count(*)
	   FROM order_statuses os1
	   WHERE os1.order_id = o.order_id AND os1.status_id = 2) = 0
	AND o.city_id = 1;

--перепишем запрос
explain analyze 
select count(*) from 
	(
	SELECT os.order_id,sum(case when status_id=2 then 1 else 0 end) pay --over(partition by os.order_id) no_pay
	FROM order_statuses os
	join(select order_id,city_id from  orders where city_id=1) o on os.order_id = o.order_id
	group by os.order_id
	order by os.order_id
)t
where pay=0;
--было 29450, стало 25

/*
можно еще создать индексы, но в данном запросе они не сильно помогли, ускорили на 2 мс
create index if not exists orders_statuses_status_id_idx on order_statuses (status_id);
create index if not exists  orders_city_id_idx on orders (city_id);*/

------- 8
-- ищет логи за текущий день
--беспричинное приведение к типу, что мешает работе индекса
explain analyze
SELECT *
FROM user_logs
WHERE datetime::date > current_date;

--убирем приведение к типу,тепрь сканирование по индексу
explain analyze
SELECT *
FROM user_logs
WHERE datetime > current_date;
--было 787, стало 0.020

------- 7 
-- ищет действия и время действия определенного посетителя
-- отсутствуют индексы в партициях-> последовательное сканирование каждой партиции 

explain analyze
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;

--создали индексы
create index if not exists  user_logs_visitor_uuid_idx on user_logs (visitor_uuid);
create index if not exists  user_logs_y2021q2_visitor_uuid_idx on user_logs_y2021q2 (visitor_uuid);
create index if not exists  user_logs_y2021q3_visitor_uuid_idx on user_logs_y2021q3 (visitor_uuid);
create index if not exists  user_logs_y2021q4_visitor_uuid_idx on user_logs_y2021q4 (visitor_uuid);

explain analyze
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;
-- было 277, стало 0.092


------- 2 
-- выводит данные о конкретном заказе: id, дату, стоимость и текущий статус
-- подзапрос в where->подзапрос повторяется столько раз, сколько строк с указанным user_id в таблице
explain analyze
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
    JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
	AND os.status_dt IN (
	SELECT max(status_dt)
	FROM order_statuses
	WHERE order_id = o.order_id
    );

--создадим индекс 
create index if not exists  order_statuses_order_id_status_id_idx on order_statuses (order_id, status_id);

--вынесем подзапрос в cte
explain analyze
with m as (SELECT order_id,max(status_dt)max_status_dt 
	FROM order_statuses	
	group by order_id)
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
    JOIN statuses s ON s.status_id = os.status_id
	join  m on os.order_id=m.order_id and os.status_dt=m.max_status_dt
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'
--было 92, стало 34

;


------- 15 
-- вычисляет количество заказов позиций, продажи которых выше среднего
--повторяемые вычисления в подзапросы
explain analyze
SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
    JOIN dishes d ON d.object_id = oi.item
WHERE oi.item IN (
	SELECT item
	FROM (SELECT item, SUM(count) AS total_sales
		  FROM order_items oi
		  GROUP BY 1) dishes_sales
	WHERE dishes_sales.total_sales > (
		SELECT SUM(t.total_sales)/ COUNT(*)
		FROM (SELECT item, SUM(count) AS total_sales
			FROM order_items oi
			GROUP BY
				1) t)
)
GROUP BY 1
ORDER BY orders_quantity DESC;

--вынесем в cte
explain analyze
with m as (
	with max_sales as (
		SELECT item, SUM(count) AS total_sales
		FROM order_items oi
		GROUP BY item) 
	select item from max_sales ms where ms.total_sales>(select sum(total_sales)/count(*) from max_sales)
	)
SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
    JOIN dishes d ON d.object_id = oi.item
join m on oi.item=m.item

GROUP BY 1
ORDER BY orders_quantity DESC;
-- было 65, стало 42
