
create schema raw_data;
create table raw_data.sales (
id int 
,auto text
,gasoline_consumption float
,price float
,date date
,person_name text
,phone text
,discount int 
,brand_origin text);

copy raw_data.sales(id,auto,gasoline_consumption,price,date,person_name,phone,discount,brand_origin) 
from 'C:\sql_practicum\cars.csv' with csv header delimiter ',' null 'null';


create schema car_shop;
--color
create table car_shop.car_color(
color_id serial primary key
,color_name varchar(30) not null --цвет, которым описывают машины обычно не длинее 30 символов
);
insert into car_shop.car_color(color_name)
select distinct trim(split_part(auto,',',2))
from raw_data.sales;

--brand_origin
create table car_shop.car_brand_origin(
origin_id serial primary key
,origin_name varchar(70) );-- самое длинное название страны на английском составляет 63 символа (The United Kingdom of Great Britain and Northern Ireland)
insert into car_shop.car_brand_origin (origin_name)
select distinct brand_origin 
from raw_data.sales
where brand_origin is not null;

--brand
create table car_shop.car_brand(
brand_id serial primary key
,brand_name varchar(50) not null--может включать и цифры, и буквы
,origin_id int references car_shop.car_brand_origin(origin_id));

insert into car_shop.car_brand(brand_name,origin_id)
select distinct trim(split_part(auto,' ',1)),origin_id
from raw_data.sales s
left join car_shop.car_brand_origin cbo on s.brand_origin=cbo.origin_name;

--model
create table car_shop.car_model(
model_id serial primary key
,model_name varchar(30) not null--модель может включать и цифры, и буквы
,brand_id int references car_shop.car_brand(brand_id)
,gasoline_consumption numeric(4,2)--не может быть трехзначным, так как салон продает только легковые автомобили
);

insert into car_shop.car_model(model_name,brand_id,gasoline_consumption)
select distinct trim(trim(split_part(split_part(s.auto,',',1),' ',2))||' '||trim(split_part(split_part(s.auto,',',1),' ',3)))
, cb.brand_id
,s.gasoline_consumption
from raw_data.sales s
left join car_shop.car_brand cb on trim(split_part(s.auto,' ',1))=cb.brand_name;

--car
create table car_shop.car_info(
car_id serial primary key
,model_id int references car_shop.car_model(model_id)
,color_id int references car_shop.car_color(color_id));

insert into car_shop.car_info (model_id,color_id)
select distinct 
cm.model_id
,color_id
from raw_data.sales s
left join car_shop.car_model cm on trim(trim(split_part(split_part(auto,',',1),' ',2))||' '||trim(split_part(split_part(auto,',',1),' ',3)))=cm.model_name
left join car_shop.car_color cc on trim(split_part(s.auto,',',2))=cc.color_name;

--customer
create table car_shop.customer(
customer_id serial primary key
,full_name varchar(100) unique not null
,phone_number varchar(30) unique not null);--включает не только цифры, но и символы +,-,(,),x

insert into car_shop.customer (full_name,phone_number)
select distinct person_name
,phone 
from raw_data.sales ;
select * from car_shop.customer;

--sales
create table car_shop.sales_info(
sale_id serial primary key
,car_id int references car_shop.car_info(car_id) not null
,customer_id int references car_shop.customer(customer_id) not null
,date_sale date not null default current_date
,discount int default 0 check (discount<100)
--,original_price numeric(9,2) not null  check (original_price>=0)--цена не может быть отрицательной, не больше 7 знаков до запятой
,total_price numeric(9,2) not null);--не больше 7 знаков до запятой


insert into car_shop.sales_info(car_id,customer_id,date_sale,discount,total_price)
select 
ci.car_id
,c.customer_id
,s.date
,s.discount
--,s.price*100/(100-s.discount)
,s.price
from car_shop.car_info ci
left join car_shop.car_model cm on ci.model_id=cm.model_id
left join car_shop.car_brand cb on  cm.brand_id=cb.brand_id
left join car_shop.car_color cc on ci.color_id=cc.color_id
left join raw_data.sales s on  cb.brand_name=trim(split_part(s.auto,' ',1)) and trim(cm.model_name)=trim(trim(split_part(split_part(auto,',',1),' ',2))||' '||trim(split_part(split_part(auto,',',1),' ',3))) and cc.color_name=trim(split_part(s.auto,',',2)) 
left join car_shop.customer c on s.person_name=c.full_name and s.phone=c.phone_number
left join car_shop.car_brand_origin cbo on cb.origin_id=cbo.origin_id;



--1
select round(cast(sum(case when ci.gasoline_consumption is null then 1 else 0 end)*100 as numeric)/count(*),2) nulls_percentage_gasoline_consumption 
from car_shop.car_model ci;


--2
select cb.brand_name
,date_part('year',si.date_sale)::int "year"
,round(avg(si.total_price),2)  price_avg
from car_shop.sales_info si
left join car_shop.car_info ci on si.car_id=ci.car_id
left join car_shop.car_model cm on ci.model_id=cm.model_id
left join car_shop.car_brand cb on cm.brand_id=cb.brand_id
group by cb.brand_name,date_part('year',date_sale)
order by cb.brand_name,year;

--3
select date_part('month',date_sale)::int "month"
,date_part('year',date_sale)::int "year"
,round(avg(total_price),2) price_avg
from car_shop.sales_info si
where date_part('year',date_sale)=2022
group by date_part('month',date_sale),date_part('year',date_sale);

--4
select c.full_name person,string_agg(trim(cb.brand_name||' '||cm.model_name),', ') 
from car_shop.sales_info si
left join car_shop.car_info ci on si.car_id=ci.car_id
left join car_shop.car_model cm on ci.model_id=cm.model_id
left join car_shop.car_brand cb on cm.brand_id=cb.brand_id
left join car_shop.customer c on si.customer_id=c.customer_id
group by full_name
order by person;


--5
select cbo.origin_name brand_origin
,round(max(si.total_price*100/(100-si.discount)),2) price_max
,round(min(si.total_price*100/(100-si.discount)),2) price_min
from car_shop.sales_info si
left join car_shop.car_info ci on si.car_id=ci.car_id
left join car_shop.car_model cm on ci.model_id=cm.model_id
left join car_shop.car_brand cb on cm.brand_id=cb.brand_id
left join car_shop.car_brand_origin cbo on cb.origin_id=cbo.origin_id
where cbo.origin_name is not null
group by cbo.origin_name;

--6
select count(*)::int persons_from_usa_count from car_shop.customer
where phone_number like '+1%';




