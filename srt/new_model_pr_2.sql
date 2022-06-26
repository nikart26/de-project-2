drop table if exists shipping_info ;
drop table if exists shipping_country_rates;
drop table if exists shipping_agreement ;
drop table if exists shipping_transfer ;
drop table if exists shipping_status;

create table shipping_country_rates(
	shipping_country_id serial primary key,
	shipping_country text,
	shipping_country_base_rate numeric(14,2)
	);

insert into shipping_country_rates (shipping_country, shipping_country_base_rate)
select distinct 
	shipping_country,
	shipping_country_base_rate	
from shipping s ;

--Проверка заполнения
--select * from shipping_country_rates limit 10;

--2--------------------------------------------------------------------------------

create table shipping_agreement (
		agreementid int8 primary key,
		agreement_number text ,
		agreement_rate numeric (14, 2),
		agreement_commission numeric (14, 2)
		 )
		 ;

insert into shipping_agreement 
(agreementid, agreement_number, agreement_rate, agreement_commission)
	select 
		q.splited[1]::int,
		q.splited[2]::text,
		q.splited[3]::numeric (14, 2),
		q.splited[4]::numeric (14, 2)
	from (select distinct regexp_split_to_array(vendor_agreement_description, E'\\:+')as splited from shipping s) q --есть дубли, берем только уникальные
	;

--Проверка заполнения
select * from shipping_agreement limit 10;

--3---------------------------------------------------------------------------------

create table shipping_transfer(
	transfer_type_id serial primary key,
	transfer_type text,
	transfer_model text,
	shipping_transfer_rate numeric (14, 3)
	)
;

insert into shipping_transfer 
(transfer_type, transfer_model, shipping_transfer_rate)
	select distinct 
		(regexp_split_to_array(shipping_transfer_description, E'\\:+'))[1] as transfer_type,
		(regexp_split_to_array(shipping_transfer_description, E'\\:+'))[2] as transfer_model,
		shipping_transfer_rate
	from shipping s 
;

-- Проверка заполнения таблицы
select * from shipping_transfer;

--4----------------------------------------------------

create table shipping_info (
shippingid int8 primary key,
vendorid int8,
payment_amount numeric (14, 2),
shipping_plan_datetime timestamp,
transfer_type_id bigint,
shipping_country_id bigint,
agreementid int8,
foreign key (transfer_type_id) references shipping_transfer(transfer_type_id) on update cascade,
foreign key (shipping_country_id) references shipping_country_rates(shipping_country_id) on update cascade,
foreign key (agreementid) references shipping_agreement(agreementid) on update cascade
)
;

insert into shipping_info
(shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
	select distinct  
		s.shippingid as shippingid, 
		s.vendorid as vendorid, 
		s.payment_amount as payment_amount, 
		s.shipping_plan_datetime as shipping_plan_datetime,
		st.transfer_type_id as transfer_type_id,
		scr.shipping_country_id as shipping_country_id,
		(regexp_split_to_array(s.vendor_agreement_description, E'\\:+'))[1]::int8 as agreementid	
	from shipping s 
	left join shipping_transfer st on st.transfer_type ||':'||st.transfer_model = s.shipping_transfer_description 
	left join shipping_country_rates scr on scr.shipping_country = s.shipping_country 
	;
--Проверка заполнения
--select * from shipping_info limit 10;

--5----------------------------------------------------------------------------------------

create table shipping_status (
shippingid int8 primary key,
status text,
state text,
shipping_start_fact_datetime timestamp,
shipping_end_fact_datetime timestamp
)
;

insert into shipping_status
(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
with 
cs as(
	select 
		shippingid, 
		status, 
		state, 
		state_datetime,
		row_number () over(partition by shippingid order by state_datetime desc) as rn
	from shipping s),	
ss as (
	select 
			shippingid as shippingid,
			state_datetime as shipping_start_fact_datetime
	from shipping s 
	where state = 'booked'),	
es as (
	select 
			shippingid as shippingid,
			state_datetime as shipping_end_fact_datetime 
	from shipping s 
	where state = 'recieved')
select 
	cs.shippingid as shippingid, 
	cs.status as status, 
	cs.state as state,
	ss.shipping_start_fact_datetime as shipping_start_fact_datetime,
	es.shipping_end_fact_datetime as shipping_end_fact_datetime	
	from cs left join ss on ss.shippingid = cs.shippingid
			left join es on es.shippingid = cs.shippingid
	where cs.rn=1 
	order by shippingid 
;

--Проверка заполнения
--select * from shipping_status limit 10;

--6--------------------------------------------------------------------------------

create or replace view shipping_datamart as (
select 
	ss.shippingid as shippingid,
	si.vendorid as vendorid,
	st.transfer_type as transfer_type,
	case when ss.shipping_end_fact_datetime is not null then 
		floor((extract (epoch from age(ss.shipping_end_fact_datetime, ss.shipping_start_fact_datetime)))/84600)
		else null end as full_day_at_shipping,
	case when ss.shipping_end_fact_datetime is not null then
			(case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 else 0 end)
		else null end as is_delay,
	case when ss.status = 'finished' then 1 else 0 end as is_shipping_finish,
	case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 
			floor((extract (epoch from age(ss.shipping_end_fact_datetime, si.shipping_plan_datetime)))/84600)
		else 0 end as delay_day_at_shipping,
	si.payment_amount as payment_amount,
	si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) as vat,
	si.payment_amount * sa.agreement_commission as profit
from shipping_status ss 
	left join shipping_info si on si.shippingid =ss.shippingid 
	left join shipping_transfer st on st.transfer_type_id = si.transfer_type_id 
	left join shipping_country_rates scr on scr.shipping_country_id = si.shipping_country_id 
	left join shipping_agreement sa on sa.agreementid = si.agreementid 
	);

-- Проверки
--select
--	(select sum(payment_amount) from shipping_datamart) as payment_dm,
--	(select sum(payment_amount) from (select distinct shippingid, payment_amount from shipping) as payment_init),
--	(select count(distinct shippingid) from shipping_datamart) as shipping_dm,
--	(select count(distinct shippingid) from shipping) as shipping_init
--	;
-- Проверки проходят успешно, данные не потеряны и не задвоены
