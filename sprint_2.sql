
create type cafe.restaurant_type as enum ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

create table cafe.restaurants (restaurant_uuid  uuid primary key default gen_random_uuid() 
,cafe_name varchar(50)
,cafe_location geometry(point,4326)
,cafe_type cafe.restaurant_type
,menu json);

insert into cafe.restaurants (cafe_name,cafe_location,cafe_type,menu)
select  distinct s.cafe_name,st_makepoint( longitude,latitude),cast(type as cafe.restaurant_type) ,menu
from raw_data.sales s
left join raw_data.menu m on s.cafe_name=m.cafe_name
order by cafe_name;


create table cafe.managers (manager_uuid uuid primary key default gen_random_uuid()
,manager varchar(50)
,manager_phone varchar(50));

insert into cafe.managers (manager,manager_phone)
select distinct manager,manager_phone from  raw_data.sales;

create table cafe.restaurant_manager_work_dates (restaurant_uuid uuid references cafe.restaurants (restaurant_uuid) 
,manager_uuid uuid references cafe.managers (manager_uuid)
,start_date date
,end_date date
,primary key (restaurant_uuid,manager_uuid));

insert into cafe.restaurant_manager_work_dates(restaurant_uuid,manager_uuid,start_date,end_date)
select distinct restaurant_uuid,manager_uuid,min(report_date),max(report_date) from raw_data.sales s
left join cafe.restaurants r on s.cafe_name=r.cafe_name
left join cafe.managers m on s.manager=m.manager
group by restaurant_uuid,manager_uuid;

create table cafe.sales (restaurant_uuid uuid references cafe.restaurants (restaurant_uuid)
,avg_check numeric
,date date
,primary key (restaurant_uuid,date));

insert into cafe.sales (date,restaurant_uuid,avg_check)
select distinct report_date,restaurant_uuid,avg_check from raw_data.sales s
left join cafe.restaurants r on s.cafe_name=r.cafe_name;

--1

create view top_3_cafe as
select cafe_name "Название заведения"
,cafe_type "Тип заведения"
,avg_  "Средний чек"
from (
	select cafe_name
	,cafe_type
	,avg_
	,row_number() over(partition by cafe_type order by avg_ desc) rn 
	from (
		select cafe_name
		,cafe_type
		,round(avg(avg_check),2) avg_--,max(avg_check) over(partition by cafe_type order by cafe_type desc) rn 
		from cafe.sales s
		left join cafe.restaurants r on s.restaurant_uuid=r.restaurant_uuid
		group by cafe_name,cafe_type
	) t
)k
where rn<=3
order by cafe_type,avg_ desc;

select * from top_3_cafe;


--2
create materialized view cafe_throught_years as
select yy"Год"
,cafe_name "Название заведения"
,cafe_type "Тип заведения"
,avg_ "Средний чек в этом году"
,lag(avg_,1,null) over o1"Средний чек в предыдущем году"
,round((avg_-lag(avg_,1,null) over o1)*100/lag(avg_,1,null) over o1,2)"Изменение среднего чека в %" from (
	select distinct extract(year from date) yy
	,cafe_name
	,cafe_type
	,round(avg(avg_check),2) avg_
	--,lead(round(avg(avg_check),2)) over(partition by extract(year from date),cafe_name)--,max(avg_check) over(partition by cafe_type order by cafe_type desc) rn 
	from cafe.sales s
	left join cafe.restaurants r on s.restaurant_uuid=r.restaurant_uuid
	where extract(year from date)<>2023
	group by cafe_name,cafe_type,extract(year from date)--left(date,4)
) t

window o1 as  (partition by cafe_name order by yy);
select * from cafe_throught_years;

--3

select cafe_name "Название заведения"
,count(distinct manager_uuid) "Сколько раз менялся менеджер" 
from cafe.restaurant_manager_work_dates d
left join cafe.restaurants r on d.restaurant_uuid=r.restaurant_uuid
group by cafe_name
order by "Сколько раз менялся менеджер" desc
limit 3 ;


--4
--вариант 1
select cafe_name"Название заведения"
,count_pi "Количество пицц в меню"
from (
	
	select cafe_name
	,count_pi
	,max(count_pi) over() max_pi from (
		select cafe_name
		,count(pi) count_pi from (
			select cafe_name
			,json_each(menu->'Пицца') pi 
			from cafe.restaurants
			where cafe_type='pizzeria'
		)t
		group by cafe_name
	)r
	
	group by cafe_name,count_pi
)y
where count_pi=max_pi;

--вариант 2
with pizza_count as (
select cafe_name
		,count(pi) count_pi,dense_rank() over(order by count(pi) desc ) dr
		from (
			select cafe_name
			,json_each(menu->'Пицца') pi 
			from cafe.restaurants
			where cafe_type='pizzeria'
		)t
		group by cafe_name)
select cafe_name"Название заведения"
,count_pi "Количество пицц в меню"
from pizza_count where dr=1;


--5
select cafe_name "Название заведения"
,'Пицца' as "Тип блюда"
,pi_name "Название пиццы"
,max_price "Цена"
from (
	select cafe_name
	,pi_name
	,pi_price
	,max(pi_price) over(partition by cafe_name) max_price
	from (
		select cafe_name
		,(json_each_text(menu->'Пицца')).key pi_name
		,cast((json_each_text(menu->'Пицца')).value as numeric) pi_price
		from cafe.restaurants
		where cafe_type='pizzeria'
		)t
	group by cafe_name,pi_name,pi_price
)f
where pi_price=max_price
order by cafe_name;



--6
select cf1"Название Заведения 1"
,cf2"Название Заведения 2"
,cafe_type"Тип заведения"
,min_dist "Расстояние"
from (
	select cf1
	,cf2
	,cafe_type
	,dist
	,min(dist) over() min_dist 
	from (
		select r1.cafe_name cf1
		,r2.cafe_name cf2
		,r1.cafe_type
		,st_distance(r1.cafe_location::geography,r2.cafe_location::geography) dist 
		from  cafe.restaurants r1
		join cafe.restaurants r2 on r1.cafe_type=r2.cafe_type
	)t
	where dist>0 
	group by cf1,cf2,cafe_type,dist
)k
where dist=min_dist
limit 1;


--7
with cafe_in_distr as (
	select district_name
	,count(cafe_name) count_ 
	from cafe.restaurants r
	join  cafe.districts d on st_within(r.cafe_location::geometry,d.district_geom::geometry)
	group by district_name
	order by count_ desc
)
select district_name "Название района"
,count_ "Количество заведений"
from cafe_in_distr
where count_=(select max(count_) from cafe_in_distr) or count_=(select min(count_) from cafe_in_distr)
group by district_name,count_
order by count_ desc;


