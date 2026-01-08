-- --1
 CREATE OR REPLACE PROCEDURE UPDATE_EMPLOYEES_RATE (P_INFO JSON) LANGUAGE PLPGSQL AS $$
 declare _percent_rate numeric;
 _employee_id uuid;
 _min_rate int;
 i json;
 begin
if json_array_length(p_info)<=0 then raise exception 'request is empty';        
else
 for i in (select json_array_elements(p_info)) loop
 _employee_id=i->>'employee_id';
 _percent_rate=i->>'rate_change';
 _min_rate =500;
 	update employees
 	set rate=(select case when rate*(1+_percent_rate/100)>500 then rate*(1+_percent_rate/100)
	 else _min_rate end rate where id=_employee_id)
	 where id=_employee_id;
	 end loop;
end if;
 end;
 $$;
-- --2

 create or replace procedure indexing_salary(p_percent int)
 language plpgsql
 as $$
 declare _avg_rate int;
 _dop_rate_percent int=2;
 begin
 select avg(rate) into _avg_rate from employees;
 update employees
 set rate= (select case when rate>_avg_rate then (rate*(1+(p_percent::numeric/100)::numeric))::int
					else (rate*(1+((p_percent+_dop_rate_percent)::numeric/100)))::int  end rate_);
 end;
 $$;

--3
create or replace procedure close_project(p_uuid uuid)
language plpgsql as $$
declare t_f boolean;
time_w numeric;
estim_time numeric;
count_workers numeric;
log_hours numeric;
new_log_hours numeric;
emp uuid;
begin
raise notice '%', p_uuid;
t_f=(select is_active from projects where id=p_uuid::uuid);
time_w=(select sum(work_hours) from logs where project_id=p_uuid);
estim_time=(select estimated_time from projects where id=p_uuid::uuid);
count_workers =(select count(distinct employee_id) from logs where project_id=p_uuid);

if t_f=False then raise exception'Project closed';

else
	update projects
	set is_active=False
	where id=p_uuid::uuid;
	
	if estim_time is not null and estim_time>time_w
		then log_hours=(estim_time-time_w)*0.75/count_workers;
		if log_hours>16
			then new_log_hours=16;
		else 
			new_log_hours=floor(log_hours);
		end if;
			insert into logs(employee_id,project_id,work_date,work_hours)
			select distinct employee_id,project_id,current_date,new_log_hours from logs where project_id=p_uuid::uuid;
			--for emp in (select distinct employee_id from logs where project_id=p_uuid::uuid) loop
         --insert into logs(employee_id,project_id,work_date,work_hours)
			--values(emp,p_uuid::uuid,current_date,new_log_hours);
		--end loop;
	else
		raise notice 'no bonus hours';
    end if;
end if;
end;
$$;



--4
create or replace procedure log_works(p_e_id uuid,p_p_id uuid,p_date date,p_worked_hours int)
language plpgsql
as $$
declare
need_review boolean;
begin
if (select is_active from projects where id=p_p_id::uuid)=False then raise exception'Project closed';
else 
	if p_worked_hours not between 1 and 24 then
	raise notice 'Not possible work hours';
	else 
		if p_worked_hours>16
		then need_review=True;
		raise notice 'too much work hours';
		elseif p_date>current_date
		then need_review=True;
		raise notice 'future date work';
		elseif p_date<current_date-'7 days'::interval
		then need_review=True;
		raise notice 'earlier then 1 week from today';
		else need_review=false;
		end if;
	end if;
insert into logs (employee_id,project_id,work_date,work_hours,required_review)
values(p_e_id,p_p_id,p_date,p_worked_hours,need_review);

end if;
end;
$$;


--5
drop table if exists employee_rate_history;
create  table employee_rate_history(
id uuid primary key default gen_random_uuid(),
employee_id uuid references employees(id),
rate numeric,
from_date date default current_date
);

insert into employee_rate_history(employee_id,rate,from_date)
select id,rate,'2020-12-16' from employees ;


create or replace function save_employee_rate_history()
returns trigger
language plpgsql
as $$
begin
if old.rate is distinct from new.rate then
insert into employee_rate_history(employee_id,rate)
values(new.id,new.rate) ;
end if;

return null;
end;
$$;


create or replace trigger change_employee_rate
after insert or update on employees
for each row
execute function save_employee_rate_history();


--6
drop function if exists  best_project_workers ;

create or replace function best_project_workers(p_id uuid)
returns table(employee_name text,worked_hours int)
language plpgsql
as $$
begin
return query
select e.name employee_name,sum(work_hours)::int worked_hours 
from logs l
left join employees e on l.employee_id=e.id 
where project_id=p_id
group by e.name
limit 3;
end;
$$;


--6*

drop function if exists best_project_workers ;

create or replace function best_project_workers(p_id uuid)
returns table(employee_name text,worked_hours_ int)
language plpgsql
as $$
begin
return query

with sel as (
select e.name,sum(work_hours)::int worked_hours 
,count(distinct work_date)  count_days
,floor(random()*100)  rn
from logs l
left join employees e on l.employee_id=e.id 
where project_id= p_id
group by e.name
order by worked_hours desc,count_days desc)
select name,worked_hours from sel
order by worked_hours desc,count_days desc,rn desc
limit 3;
end;
$$;


--7
create or replace function calculate_month_salary(p_date_start date,p_date_end date)
returns table (id uuid,employee text,worked_hours int,salary numeric)
language plpgsql
as $$
declare p_normal_hours int=160;
p_overtime_coef float=1.25;
begin

return query
select  employee_id id,e.name employee,sum(work_hours)::int worked_hours,
case when sum(work_hours)>p_normal_hours 
then  ((sum(work_hours) -p_normal_hours)*p_overtime_coef*e.rate+e.rate*p_normal_hours)::numeric
else (sum(work_hours) *e.rate)::numeric end salary
from logs l
left join employees e on l.employee_id=e.id
where work_date between p_date_start::Date and p_date_end::date
and is_paid=False and required_review=false
group by employee_id,e.name,e.rate;
end;
$$;

--7*
create or replace function calculate_month_salary(p_date_start date,p_date_end date)
returns table (id uuid,employee text,worked_hours int,salary numeric)
language plpgsql
as $$
declare p_normal_hours int=160;
p_overtime_coef float=1.25;
i uuid;
begin
for i in (select distinct employee_id from logs 
where work_date between p_date_start::Date and p_date_end::date 
and required_review=true) loop
raise notice 'Warning! Employee % hours must be reviewed!',i;
end loop;

return query
select  employee_id id,e.name employee,sum(work_hours)::int worked_hours,
case when sum(work_hours)>p_normal_hours 
then  ((sum(work_hours) -p_normal_hours)*p_overtime_coef*e.rate+e.rate*p_normal_hours)::numeric
else (sum(work_hours) *e.rate)::numeric end salary
from logs l
left join employees e on l.employee_id=e.id
where work_date between p_date_start::Date and p_date_end::date
and is_paid=False and required_review=false
group by employee_id,e.name,e.rate;
end;
$$;
