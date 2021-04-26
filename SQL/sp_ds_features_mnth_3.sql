show procedure sbx_retail_ss.sp_ds_features_mnth_3;

replace procedure sbx_retail_ss.sp_ds_features_mnth_3 ( rep_dt date)
	sql security creator 
begin


declare v_start_dt date;
declare v_end_dt date;
declare step int default 0;
declare err_msg varchar(255);


declare exit handler for sqlexception	
	begin
		get diagnostics exception 1 err_msg=message_text;
		--- Запись в лог если на каком-то этапе процедура падает
		insert into sbx_retail_ss_core.A19_log_features_mnth_3
		values
		('SBX_RETAIL_SS.ft_ds_features_mnth_3',:step,:err_msg,0,current_timestamp(0));
		resignal;
	end;


set v_start_dt = trunc(cast(rep_dt as date),'MONTH');
set v_end_dt = last_day(v_start_dt);


-- Фиксация старта
insert into sbx_retail_ss_core.A19_log_features_mnth_3
values
('SBX_RETAIL_SS.ft_ds_features_mnth_3',-1,'v_start_dt '||v_start_dt||' ; v_end_dt '||v_end_dt, 0,current_timestamp(0));


--------------------------------------------------------------------------
--					Добавление месяца
--------------------------------------------------------------------------
set step = 0;

delete from SBX_RETAIL_SS.ft_ds_features_mnth_3 where report_dt = :v_end_dt;
		

insert into SBX_RETAIL_SS.ft_ds_features_mnth_3
( client_dk, report_dt, prd_lst_prod_division_dk )
select
	cl.client_dk
   ,report_dt
   ,prd_lst_prod_division_dk
from SBX_RETAIL_DATA.ft_clnt_aggr_mnth as cl
		
where report_dt =  :v_end_dt;

--- Лог
insert into sbx_retail_ss_core.A19_log_features_mnth_3
select
	'SBX_RETAIL_SS.ft_ds_features_mnth_3'
   ,:step
   ,'Activity_Count: '||Activity_Count
   ,null
   ,current_timestamp(0)
;

collect stats column(client_dk) on SBX_RETAIL_SS.ft_ds_features_mnth_3;
collect stats column(partition) on SBX_RETAIL_SS.ft_ds_features_mnth_3;




--------------------------------------------------------------------------
--								Фича 1
----------------------------------------------------------------------------

set step = 1;

create multiset volatile  table sbol_mkb_p2p
(
    report_dt date
	,client_dk bigint
   ,dest_dk bigint
   ,sum_out float
   ,cnt_out float
) primary index ( client_dk)
on commit preserve rows;


 --- Здесь собираем данные по исходящим переводам из двух источников  
 
 insert into sbol_mkb_p2p
select
       report_dt
	   ,client_dk
       ,dest_client_dk
       ,sum(a.op_amt) as sum_out       
       ,count(1) as cnt_out
from
    	(
		-- сумма и кол-во исходях P2P переводов по МБК      
		    select
		             last_day(a.op_date) as report_dt -- Дата события (месяц целиком)
		            ,a.client_dk -- Отправитель
		            ,a.dest_client_dk-- Получатель
		            ,a.op_amt  -- Общая сумма переводов
		    from SBX_RETAIL_DATA.ft_mb_auth_oprs as a
		    where a.op_date between :v_start_dt and :v_end_dt-- Тут менять дату
		    and a.client_dk is not null -- Есть запись отправителя
		    and a.dest_client_dk is not null -- Есть запись получателя
		    and a.client_dk <> a.dest_client_dk -- Перевод не самому себе
		    and a.client_dk > 0
		    and a.requesttype = 1
		        
		    union all    
		-- сумма и кол-во исходях P2P переводов по СБОЛ  
		    select
		             last_day(a.oper_date) as report_dt -- Дата события (месяц целиком)
		            ,a.client_dk -- Отправитель
		            ,a.receiver_client_dk as dest_client_dk -- Получатель
		            ,a.oper_rur_amt as op_amt  -- Общая сумма переводов
		    from SBX_RETAIL_DATA.ft_sbol_oper as a
		    where oper_date  between :v_start_dt and :v_end_dt   -- Тут менять дату
		    and a.fin_trx_flg = 1
		    and a.client_dk is not null -- Есть запись отправителя
		    and a.receiver_client_dk is not null -- Есть запись получателя
		    and a.client_dk <> a.receiver_client_dk -- Перевод не самому себе
		    and a.client_dk > 0
		    ) as a
group by
        client_dk
       ,report_dt
       ,dest_client_dk
;

collect stats column(client_dk) on sbol_mkb_p2p;

create multiset volatile  table sbol_mkb_p2p_rslt
(
    client_dk bigint
   ,p2p_out_amt float
   ,p2p_out_qnt float
   ,p2p_in_amt float
   ,p2p_in_qnt float
) primary index ( client_dk )
on commit preserve rows;


--- Собираем таблицу для апдейта, разделяя входящие и исходящие p2p переводы
insert into sbol_mkb_p2p_rslt
(    client_dk 
   ,p2p_out_amt
   ,p2p_out_qnt 
   ,p2p_in_amt
   ,p2p_in_qnt )
select
		a.client_dk
	   ,zeroifnull(b.sum_out) as p2p_out_amt
	   ,zeroifnull(b.cnt_out) as p2p_out_qnt 
	   ,zeroifnull(c.sum_out) as p2p_in_amt 
	   ,zeroifnull(c.cnt_out) as p2p_in_qnt 
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as a
left join 
(
-- Для client_dk сумма исходящих это исходящие
select
	client_dk
   ,sum(sum_out) as sum_out
   ,sum(cnt_out) as cnt_out
 from sbol_mkb_p2p
group by
	client_dk
) as b
	on a.client_dk = b.client_dk

left join 
(
--- Для dest_dk сумма исходящих это входящие
	select
		dest_dk
	   ,sum(sum_out) as sum_out
	   ,sum(cnt_out) as cnt_out
	 from sbol_mkb_p2p
	group by
		dest_dk
) as c
	on a.client_dk = c.dest_dk

where a.report_dt = :v_end_dt
;

collect stats column(client_dk) on sbol_mkb_p2p_rslt;


--- Обновление основной таблицы добавления записей по p2p СБОЛ и МБК
update a
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as a, sbol_mkb_p2p_rslt as b
set  p2p_in_qnt = b.p2p_in_qnt,
	   p2p_in_amt = b.p2p_in_amt,
	   p2p_out_qnt = b.p2p_out_qnt,
	   p2p_out_amt = b.p2p_out_amt
where a.client_dk = b.client_dk
and a.report_dt  = :v_end_dt
;
	
	
--- Лог
insert into sbx_retail_ss_core.A19_log_features_mnth_3
select
	'SBX_RETAIL_SS.ft_ds_features_mnth_3'
   ,:step
   ,'Activity_Count: '||Activity_Count
   ,null
   ,current_timestamp(0)
;


------------------------------------------------------------------------
--								Фича 3
------------------------------------------------------------------------

set step = 3;

create multiset volatile table main
(
   app_id varchar(30)
  ,client_dk bigint
  ,created_dt date
  ,sub_subj_id bigint
  ,reg_channel_id bigint
  ,reg_channel_name varchar(50)
) primary index (  client_dk  )
on commit preserve rows
;



-- Выбираем весь необходимый объем данных по обращениям для обработки, исключая не актуальные записи, тестовые и переадресованные
insert into main
(
   app_id
  ,client_dk 
  ,created_dt 
  ,sub_subj_id 
  ,reg_channel_id 
  ,reg_channel_name
) 
	select
		app_id
	   ,client_dk
	   ,created_dt
	   ,sub_subj_id
	   ,reg_channel_id   
	   ,reg_channel_name 
	from sbx_retail_data.dm_crm_appeal 
	where created_dt between :v_start_dt and :v_end_dt 
	and req_id is not null -- ошибочные обращения
	and client_dk <> -1 -- клиент не определен
	and grp_id not in (
		1697601, -- test_2
		25201, -- test
		1697501, -- test_1
		26201 -- реструктуризация_тест
		)	
	and subj_id not in (
	152301,	-- реструктуризация_тест
	1439601,	 -- функция 12 (тест мошенничество, без опросника)
	980901,	-- тестовые звонки
	941801,	-- тестовые звонки
	1043801,	 -- им-нецелевое - тестовые звонки
	997101,	-- тестирование специальных предложений для корпоративных клиентов
	848201,	-- тестовые звонки
	1438301,	-- тестовая (не использовать!!!)
	1374501,	-- область тематики тест
	929101,	-- тестовые звонки
	848301,	-- тестовые звонки
	1411001,	-- тестовая тематика
	1054301,	-- тестовые звонки
	444701,	-- реструктуризация_тест
	1447701,	-- нештатное устройство (тест)
	144601 ,  -- им-нецелевое - тестовые звонки
	1228701,	-- только для проведения теста
	562401,	-- тестовые звонки
	1245001,	-- тестовые звонки
	1836901,	-- параметры/статус кредитного договора - тест тест тест
	1744801,	-- тест
	1000901	-- тестовые звонки
	)
	and sub_subj_id not in (
	152301,	1439601,	980901,	941801,	1043801,	997101,	848201,	1438301,	1374501,	929101,	848301,	1411001,	1054301,	444701,	1447701,
	1228701,	562401,	1245001,	1836901,	1744801,	1000901, 144601	)
	and cons_result <> 'transferred' -- по каналу еркц , обращение переадресовано
	and reg_channel_id in
	(
	1701	--премьер всп
	,2201	--масс всп
	,14977201	--комплаенс
	,1801	--vip_pb
	,2601	--вип всп
	,3001	--дб
	,2401	--еркц
	,2801	--премьер+масс всп
	,1901 -- дуд/уд тб/госб
	)
;

collect stats column(client_dk) on main;

create multiset volatile table main_request
(
   client_dk bigint
  ,report_dt date
  ,sub_subj_id int
  ,request_top_qnt bigint
  ,reg_channel_id int
  ,reg_channel_name varchar(50)
  ,appeal_qnt bigint
  ,rn_request smallint
) primary index (  client_dk)
on commit preserve rows
;

-- Подсчет кол-ва обращений по клиентам в месяц в разрезе тем по заявкам и нумерация их по популярности начиная с самой популярной
insert into main_request
(
   client_dk 
  ,report_dt 
  ,sub_subj_id 
  ,request_top_qnt
  ,rn_request
)
select
	client_dk
   ,report_dt
   ,sub_subj_id
   ,request_top_qnt
   ,row_number() over ( partition by client_dk,report_dt order by request_top_qnt desc ) as rn_request
from 
(
	select
	   client_dk 
	  ,last_day(created_dt) as report_dt 
	  ,sub_subj_id 
	  ,count(1) as  request_top_qnt
	from main
	group by
	   client_dk 
	  ,report_dt
	  ,sub_subj_id
) as a
;

create multiset volatile table main_applic
(
   client_dk bigint
  ,report_dt date
  ,reg_channel_id int
  ,reg_channel_name varchar(50)
  ,appeal_qnt bigint
) primary index ( client_dk )
on commit preserve rows
;

-- Подсчет кол-ва уникальных обращений за месяц  в разрезе каналов
insert into main_applic
(
   client_dk 
  ,report_dt 
  ,reg_channel_id
  ,reg_channel_name
  ,appeal_qnt 
) 
select
		client_dk
	   ,last_day(created_dt) as report_dt
	   ,reg_channel_id
	   ,max(reg_channel_name) as reg_channel_name
	   ,count(distinct app_id) as appeal_qnt
from main
group by 
		client_dk
	   ,report_dt
	   ,reg_channel_id
;

collect stats column(client_dk) on main_applic;

create multiset volatile table main_applic_rslt
(
   client_dk bigint
  --,report_dt date
  ,claim_topic_1_id int
  ,claim_topic_1_request_qnt int
  ,claim_topic_2_id int
  ,claim_topic_2_request_qnt int
  ,claim_request_total_qnt int
  ,claim_appeal_channel varchar(50)
  ,claim_appeal_total_qnt int
) primary index (  client_dk  )
on commit preserve rows
;

insert into main_applic_rslt
select
   a.client_dk
 -- ,a.report_dt
  ,a.sub_subj_id_1 as claim_topic_1_id
  ,a.request_top_qnt as claim_topic_1_request_qnt
  ,b.sub_subj_id_2  as claim_topic_2_id
  ,b.request_top_qnt as claim_topic_2_request_qnt
  ,c.request_total_qnt as claim_request_total_qnt
  ,d.appeal_channel_name as claim_appeal_channel
  ,e.appeal_qnt  as claim_appeal_total_qnt
from
(
--- Самая популярная тема заявок
	select
		   client_dk 
		  --,report_dt 
		  ,sub_subj_id as sub_subj_id_1
		  ,request_top_qnt 
	from main_request
	where rn_request = 1
) as a
left join
(
-- Вторая по популярности тема заявок
	select
		   client_dk 
		  --,report_dt 
		  ,sub_subj_id as sub_subj_id_2
		  ,request_top_qnt 
	from main_request
	where rn_request = 2
) as b
	on a.client_dk = b.client_dk
	--and a.report_dt = b.report_dt
left join
(
-- Общее кол-во заявок
select
		client_dk
	   --,report_dt
	   ,sum(request_top_qnt) as request_total_qnt
from main_request
group by	1
) as c
	on a.client_dk = c.client_dk
	--and a.report_dt = c.report_dt	
left join
(
-- Самый популярный канал по обращениям
select
		client_dk
	   --,report_dt
	   ,reg_channel_id as appeal_channel_id
	   ,reg_channel_name as appeal_channel_name
	   ,appeal_qnt
	from main_applic as aa
	qualify ( row_number() over ( partition by client_dk order by appeal_qnt desc )) = 1	
) as d
	on a.client_dk = d.client_dk
	--and a.report_dt = d.report_dt
left join
(
-- Общее кол-во обращений
	select
		client_dk
	   --,report_dt
	   ,sum(appeal_qnt) as appeal_qnt
	from main_applic as aa
	group by 1  
) as e
	on a.client_dk = e.client_dk
	--and a.report_dt = e.report_dt
;




-- Обновляем финальную таблицу client_dk,report_dt уникальны
update a
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as a,main_applic_rslt as b
set 
   claim_topic_1_id = b.claim_topic_1_id
  ,claim_topic_1_request_qnt = b.claim_topic_1_request_qnt
  ,claim_topic_2_id = b.claim_topic_2_id
  ,claim_topic_2_request_qnt = b.claim_topic_2_request_qnt
  ,claim_request_total_qnt = b.claim_request_total_qnt
  ,claim_appeal_channel = b.claim_appeal_channel
  ,claim_appeal_total_qnt = b.claim_appeal_total_qnt
where a.client_dk = b.client_dk
and a.report_dt = :v_end_dt
;


--- Лог
insert into sbx_retail_ss_core.A19_log_features_mnth_3
select
	'SBX_RETAIL_SS.ft_ds_features_mnth_3'
   ,:step
   ,'Activity_Count: '||Activity_Count
   ,null
   ,current_timestamp(0)
;


-------------------------------------------------------------------------
--								Фича 5
-------------------------------------------------------------------------

set step = 5;

create multiset volatile table TRX_ecom
(
	client_dk bigint
   ,report_dt date
   ,trx_ecom_qty smallint
   ,trx_ecom_amt decimal(18,2)
   ,trx_usage_days_qty smallint
) primary index ( client_dk )
on commit preserve rows
;

insert into TRX_ecom
select 
	 tx.client_dk 
	,last_day(trunc(evt_dt, 'MM')) as report_dt
			-- кол-во E-com
	,sum(case when tp.ips_txn_group_cd = 'SALES'
				and tx.ecom_fl=1
				then 1 else 0 end)
		as Trx_Ecom_qty
			-- сумма E-com
	,sum(case when tp.ips_txn_group_cd = 'SALES'
				and tx.ecom_fl=1
				then local_amt else 0 end)
		as Trx_Ecom_amt
			-- кол-во дней с активностью
	,count(distinct case when card_txn_cat_cd = 1
												and coalesce(tp.ips_txn_group_cd,'t') <> 'REFUND' -- Возвраты
									then evt_dt end)
		as Trx_Usage_days_qty
from SBX_RETAIL_DATA.ft_card_txn_det as tx
        join sbx_retail_data.ref_card_txn_type  as tp
				on tx.trans_type = tp.card_txn_type_rk
where evt_dt between :v_start_dt and :v_end_dt
and tr_type = 'C' -- Кредитные
group by 
		tx.client_dk
	   ,last_day(trunc(evt_dt, 'MM')) 
;

update a
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as a,TRX_ecom as b
	set  Trx_Ecom_qty = b.Trx_Ecom_qty
			,Trx_Ecom_amt = b.Trx_Ecom_amt
			,Trx_Usage_days_qty = b.Trx_Usage_days_qty
where a.client_dk = b.client_dk
and a.report_dt = :v_end_dt		
;

--- Лог
insert into sbx_retail_ss_core.A19_log_features_mnth_3
select
	'SBX_RETAIL_SS.ft_ds_features_mnth_3'
   ,:step
   ,'Activity_Count: '||Activity_Count
   ,null
   ,current_timestamp(0)
;

update SBX_RETAIL_SS.ft_ds_features_mnth_3
	set Trx_Ecom_qty = 0,Trx_Ecom_amt =0 ,Trx_Usage_days_qty = 0
where Trx_Ecom_qty is null
and report_dt = :v_end_dt	
;




-------------------------------------------------------------------------
--								Фича 6
-------------------------------------------------------------------------


set step = 6;

create multiset volatile table vklad
(
	dep_agrmnt_dk bigint
   ,client_dk bigint
   ,dep_close_repdt byteint
   ,dep_close_dtm1 byteint
   ,dep_close_dtm2 byteint
   ,dep_close_dtp1 byteint
   ,dep_close_dtp2 byteint
   ,dep_prlng_repdt byteint
   ,dep_prlng_dtm1 byteint
   ,dep_prlng_dtm2 byteint
) primary index ( dep_agrmnt_dk )
on commit preserve rows
;


insert into vklad
sel
	  d.dep_agrmnt_dk
	 ,d.client_dk
		--- факт закрытия вклада
,case when last_day(agrmnt_close_actual_dt) = v_end_dt	-- дата отчета
				then 1 else 0 end
		as dep_close_repdt
,case when last_day(agrmnt_close_actual_dt) = oadd_months(v_end_dt, -1)
				then 1 else 0 end
		as dep_close_dtm1
,case when last_day(agrmnt_close_actual_dt) = oadd_months(v_end_dt, -2)
				then 1 else 0 end
		as dep_close_dtm2
		
		--- плановое закрытие вклада	в будущем	
,case when agrmnt_close_actual_dt is null
				and last_day(agrmnt_close_plan_dt) = oadd_months(v_end_dt, 1)
				then 1 else 0 end
		as dep_close_dtp1
,case when agrmnt_close_actual_dt is null
				and last_day(agrmnt_close_plan_dt) = oadd_months(v_end_dt, 2)
				then 1 else 0 end
		as dep_close_dtp2
		
		--- пролонгация вклада с учетом, что он мог закрыться в тот же месяц
,case when last_day(agrmnt_prlng_last_dt) = v_end_dt	-- дата отчета
				and coalesce(agrmnt_close_actual_dt, v_end_dt+1) > v_end_dt
				then 1 else 0 end
		as dep_prlng_repdt
		--- пролонгация вклада с учетом, что он мог закрыться спустя месяц после пролонгации
,case when last_day(agrmnt_prlng_last_dt) = oadd_months(v_end_dt, -1)
				and coalesce(agrmnt_close_actual_dt, v_end_dt) > oadd_months(agrmnt_prlng_last_dt, 1)
				then 1 else 0 end
		as dep_prlng_dtm1
		--- пролонгация вклада с учетом, что он мог закрыться спустя месяц после пролонгации
,case when last_day(agrmnt_prlng_last_dt) = oadd_months(v_end_dt, -2)
				and coalesce(agrmnt_close_actual_dt, v_end_dt) > oadd_months(agrmnt_prlng_last_dt, 1)
				then 1 else 0 end
		as dep_prlng_dtm2
		
from SBX_RETAIL_DATA.dm_dep_agrmnt as d
where :v_end_dt between row_actual_from_dt and row_actual_to_dt
			and dep_product_cat_cd = 'TERM_DEPOSIT'
			-- для ограничения объема убираем старые закрывшиеся вклады
			and coalesce(agrmnt_close_actual_dt, v_end_dt) > oadd_months(v_end_dt, -3)
			and d.client_dk > 0
;

collect stats column(dep_agrmnt_dk) on vklad;


create multiset volatile table vklad2 
(
    client_dk bigint
   ,dep_close_repdt_cnt int
   ,dep_close_repdt_sum float
   ,dep_close_dtm1_cnt int
   ,dep_close_dtm2_cnt int
   ,dep_close_dtp1_cnt int
   ,dep_close_dtp2_cnt int
   ,dep_prlng_repdt_cnt int
   ,dep_prlng_dtm1_cnt int
   ,dep_prlng_dtm2_cnt int
) primary index ( client_dk )
on commit preserve rows
;


	-- добавляем сумму закрытых вкладов и агрегируем на клиента
insert into vklad2
select
		v1.client_dk
		,sum(v1.dep_close_repdt) as dep_close_repdt_cnt
		,sum(dagr.txn_otf_total_rub_amt) as dep_close_repdt_sum
		,sum(v1.dep_close_dtm1) as   	 dep_close_dtm1_cnt
		,sum(v1.dep_close_dtm2)  as       dep_close_dtm2_cnt       
		,sum(v1.dep_close_dtp1) as       dep_close_dtp1_cnt       
		,sum(v1.dep_close_dtp2)  as 		dep_close_dtp2_cnt              
		,sum(v1.dep_prlng_repdt)   as 	dep_prlng_repdt_cnt            
		,sum(v1.dep_prlng_dtm1)   as 	dep_prlng_dtm1_cnt          
		,sum(v1.dep_prlng_dtm2) as 		dep_prlng_dtm2_cnt
from vklad as v1
		left join SBX_RETAIL_DATA.ft_dep_agrmnt_aggr as dagr
			on dagr.dep_agrmnt_dk = v1.dep_agrmnt_dk
			and dagr.report_dt = :v_end_dt
			and v1.dep_close_repdt = 1
group by 
		v1.client_dk
where v1.client_dk > 0
;


collect stats column(client_dk) on vklad2;

create multiset volatile table vklad3
(
    client_dk bigint
   ,curr_acc_repdt_sum decimal(18,2)
) primary index ( client_dk )
on commit preserve rows
;

	---	 сумма на текущих счетах
insert into vklad3
sel
	 d.client_dk
	,sum(dagr.dep_curr_bal_rub_amt)(DECIMAL(18,2)) as curr_acc_repdt_sum
group by 1
from SBX_RETAIL_DATA.dm_dep_agrmnt as d
		join SBX_RETAIL_DATA.ft_dep_agrmnt_aggr as dagr
			on dagr.dep_agrmnt_dk = d.dep_agrmnt_dk
			and dagr.report_dt = :v_end_dt
where :v_end_dt between row_actual_from_dt and row_actual_to_dt
			and d.dep_product_cat_cd = 'CURRENT_ACCOUNT'
			and d.client_dk > 0
;


collect stats column(client_dk) on vklad3;

create multiset volatile table  vklad_rslt
(
	client_dk bigint
	,dep_close_repdt_cnt  smallint
	,dep_close_repdt_sum decimal(18,2)
	,dep_close_dtm1_cnt smallint
	,dep_close_dtm2_cnt smallint
	,dep_close_dtp1_cnt smallint
	,dep_close_dtp2_cnt smallint
	,dep_prlng_repdt_cnt  smallint
	,dep_prlng_dtm1_cnt smallint
	,dep_prlng_dtm2_cnt smallint
	,curr_acc_repdt_sum decimal(18,2)
) primary index ( client_dk )
on commit preserve rows
;


insert into  vklad_rslt
(
	 client_dk 
	,dep_close_repdt_cnt
	,dep_close_repdt_sum 
	,dep_close_dtm1_cnt
	,dep_close_dtm2_cnt
	,dep_close_dtp1_cnt 
	,dep_close_dtp2_cnt 
	,dep_prlng_repdt_cnt
	,dep_prlng_dtm1_cnt 
	,dep_prlng_dtm2_cnt 
	,curr_acc_repdt_sum 
)
select
	 f.client_dk
	,coalesce(v2.dep_close_repdt_cnt,0)
	,coalesce(v2.dep_close_repdt_sum,0)
	,coalesce(v2.dep_close_dtm1_cnt,0)
	,coalesce(v2.dep_close_dtm2_cnt,0)
	,coalesce(v2.dep_close_dtp1_cnt,0)
	,coalesce(v2.dep_close_dtp2_cnt,0)
	,coalesce(v2.dep_prlng_repdt_cnt,0)
	,coalesce(v2.dep_prlng_dtm1_cnt,0)
	,coalesce(v2.dep_prlng_dtm2_cnt,0)
	,coalesce(v3.curr_acc_repdt_sum,0)
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as f
		left join vklad2 as v2
				on v2.client_dk = f.client_dk

		left join vklad3 as v3
				on v3.client_dk = f.client_dk

where f.report_dt = :v_end_dt
;

update a
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as a,vklad_rslt as b
	set dep_close_repdt_cnt = b.dep_close_repdt_cnt
		  ,dep_close_repdt_sum = b.dep_close_repdt_sum
		  ,dep_close_dtm1_cnt = b.dep_close_dtm1_cnt
		  ,dep_close_dtm2_cnt = b.dep_close_dtm2_cnt
		  ,dep_close_dtp1_cnt = b.dep_close_dtp1_cnt
		  ,dep_close_dtp2_cnt = b.dep_close_dtp2_cnt
		  ,dep_prlng_repdt_cnt = b.dep_prlng_repdt_cnt
		  ,dep_prlng_dtm1_cnt = b.dep_prlng_dtm1_cnt
		  ,dep_prlng_dtm2_cnt = b.dep_prlng_dtm2_cnt
		  ,curr_acc_repdt_sum = b.curr_acc_repdt_sum
where a.client_dk = b.client_dk
and a.report_dt = :v_end_dt
;


--- Лог
insert into sbx_retail_ss_core.A19_log_features_mnth_3
select
	'SBX_RETAIL_SS.ft_ds_features_mnth_3'
   ,:step
   ,'Activity_Count: '||Activity_Count
   ,null
   ,current_timestamp(0)
;



-------------------------------------------------------------------------
--								Фича 7
-------------------------------------------------------------------------


set step = 7;

create multiset volatile  table credits 
(
	client_dk bigint
   ,report_dt date
   ,cred_apply_flag byteint
   ,cred_pl_reject byteint
   ,cred_pl_approve byteint
   ,cred_mg_reject byteint
   ,cred_mg_approve byteint
   ,cred_cc_reject byteint
   ,cred_cc_approve byteint
) primary index ( client_dk )
on commit preserve rows
;

--- Клиенты подававшие заявку на кредит
insert into credits
select
		a.client_dk
	   ,a.report_dt
	   ,case when b.client_dk is null then 0 else 1 end as cred_apply_flag -- В подзапросе только те клиенты которые подавали заявку
	   ,b.cred_pl_reject
	   ,b.cred_pl_approve
	   ,b.cred_mg_reject
	   ,b.cred_mg_approve
	   ,b.cred_cc_reject
	   ,b.cred_cc_approve
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as a
left join
(
select 
	 client_dk
	,last_day(req_dt) as report_dt
	,max(case when cred_type_desc='Потребительское кредитование'
			and request_status_desc='Отказ' then 1 else 0 end) cred_pl_reject
	,max(case when cred_type_desc='Потребительское кредитование'
			and request_status_desc='Кредит выдан' then 1 else 0 end) cred_pl_approve
	,max(case when cred_type_desc='Жилищное кредитование'
			and request_status_desc='Отказ' then 1 else 0 end) cred_mg_reject
	,max(case when cred_type_desc='Жилищное кредитование'
			and request_status_desc='Кредит выдан' then 1 else 0 end) cred_mg_approve
	,max(case when cred_type_desc='Банковские карты'
			and request_status_desc='Отказ' then 1 else 0 end) cred_cc_reject
	,max(case when cred_type_desc='Банковские карты'
			and request_status_desc='Кредит выдан' then 1 else 0 end) cred_cc_approve
from SBX_RETAIL_DATA.RD_Transact_data 
where req_dt between :v_start_dt and :v_end_dt
group by 
	 client_dk
	,last_day(req_dt)
) as b
on a.client_dk = b.client_dk
where a.report_dt = :v_end_dt
;


update a
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as a,credits as b
set
   cred_apply_flag = b.cred_apply_flag
  ,cred_pl_reject = b.cred_pl_reject
  ,cred_pl_approve = b.cred_pl_approve
  ,cred_mg_reject = b.cred_mg_reject
  ,cred_mg_approve = b.cred_mg_approve
  ,cred_cc_reject = b.cred_cc_reject
  ,cred_cc_approve = b.cred_cc_approve
where a.client_dk = b.client_dk
and a.report_dt = :v_end_dt
;


--- Лог
insert into sbx_retail_ss_core.A19_log_features_mnth_3
select
	'SBX_RETAIL_SS.ft_ds_features_mnth_3'
   ,:step
   ,'Activity_Count: '||Activity_Count
   ,null
   ,current_timestamp(0)
;



-------------------------------------------------------------------------
--								Фича 8
-------------------------------------------------------------------------

set step = 8;

create multiset volatile table bnk_crd
(
   client_dk bigint
  ,report_dt date
  ,final_bank varchar(255)
  ,cnt_amt int
  ,doc_amt float
) primary index ( client_dk )
on commit preserve rows;


-- Собираем таблицу фичей по самому популярному стороннему банку
insert into bnk_crd
select
  a.client_dk
,a.report_dt
,a.final_bank
,a.cnt_amt
,a.doc_amt
from
(
    select
      client_dk
     ,last_day(pmt_create_dt) as report_dt
     ,final_bank
     ,sum(doc_amt) as doc_amt
     ,count(*) as cnt_amt
    from SBX_RETAIL_DATA.dm_client_knlg_other_bank_credit
    where pmt_create_dt between :v_start_dt and :v_end_dt 
    and final_bank <> '' ''
    group by
      client_dk
     ,last_day(pmt_create_dt)
     ,final_bank
) as a
qualify ( row_number () over (partition by client_dk, report_dt order by doc_amt desc) ) = 1
;




-- Обновляем таблицу фичей по самому популярному стороннему банку
update a
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as a,bnk_crd as b
set 
	 other_bank = b.final_bank
	,other_bank_pay_cnt = b.cnt_amt
	,other_bank_pay_amt = b.doc_amt
where a.client_dk = b.client_dk
and a.report_dt = :v_end_dt
;


--- Лог
insert into sbx_retail_ss_core.A19_log_features_mnth_3
select
	'SBX_RETAIL_SS.ft_ds_features_mnth_3'
   ,:step
   ,'Activity_Count: '||Activity_Count
   ,null
   ,current_timestamp(0)
;



-------------------------------------------------------------------------
--								Фича 9
-------------------------------------------------------------------------

set step = 9;

create multiset volatile table family_list
(
	client_dk bigint
   ,report_dt date
   ,lifestyle_family varchar(512)
) primary index ( client_dk )
on commit preserve rows
;

-- Находим список состава семьи по клиентам
insert into family_list
select 
	a.client_dk
   ,a.report_dt
   ,a.lifestyle_family
from SBX_RETAIL_DATA.dm_client_knowledge as a
inner join SBX_RETAIL_SS.ft_ds_features_mnth_3 as b
	on a.client_dk = b.client_dk
	and a.report_dt = b.report_dt
where a.report_dt =  :v_end_dt
and b.report_dt =  :v_end_dt
and a.lifestyle_family <> '0'
;


create multiset volatile table family_list_single
(
	client_dk bigint
   ,report_dt date
   ,lifestyle_family_dk bigint
) primary index ( lifestyle_family_dk )
on commit preserve rows
;

-- Приводим список состава семьи клиента в нормализованный вид. Из строки в столбец
insert into family_list_single
select 
	a.client_dk
   ,a.report_dt
   ,b.lifestyle_family_dk
from family_list as a
inner join
(
	select 
		 d.outkey as client_dk
		,cast(d.token as int) as lifestyle_family_dk
	FROM TABLE (
			strtok_split_to_table(cast(family_list.client_dk as int),family_list.lifestyle_family, ',')
	RETURNS (outkey int, tokennum int, token varchar(9)) 
			) as d
	where d.outkey <> d.token
) as b
on a.client_dk = b.client_dk
;



create multiset volatile table family_list_rslt
(
	 client_dk bigint
    ,report_dt date
	,family_qnt int
	,family_dc_activity_ind float
	,family_cc_activity_ind float
	,family_da_activity_ind float
	,family_pl_activity_ind float
	,family_mg_activity_ind float
	,family_client_activity_ind float
	,family_ib_activity_1m_ind float
	,family_mb_activity_1m_ind float
	,family_age_yrs_comp_nv float
	,family_payroll_client_nflag float
	,family_inc_avg_risk_rub_amt float
	,family_otf_pos_spend_rub_amt float
	,family_otf_pos_spend_qty float
) primary index ( client_dk )
on commit preserve rows
;


-- Собираем таблицу по средним значения за месяц по семье
insert into family_list_rslt
select
  m.client_dk
 ,m.report_dt
 ,count(*) as family_qnt
 ,avg(m.prd_dc_activity_ind) as family_dc_activity_ind
 ,avg(m.prd_cc_activity_ind) as family_cc_activity_ind
 ,avg(m.prd_da_activity_ind) as family_da_activity_ind
 ,avg(m.prd_pl_activity_ind) as family_pl_activity_ind
 ,avg(m.prd_mg_activity_ind) as family_mg_activity_ind
 ,avg(m.prd_client_activity_ind) as family_client_activity_ind
 ,avg(m.prd_ib_activity_1m_ind) as family_ib_activity_1m_ind
 ,avg(m.prd_mb_activity_1m_ind) as family_mb_activity_1m_ind
 ,avg(m.sd_age_yrs_comp_nv) as family_age_yrs_comp_nv
 ,avg(m.lbt_payroll_client_nflag) as family_payroll_client_nflag
 ,avg(m.lbt_inc_avg_risk_rub_amt) as family_inc_avg_risk_rub_amt
 ,avg(m.crd_otf_pos_spend_rub_amt) as family_otf_pos_spend_rub_amt
 ,avg(m.crd_otf_pos_spend_qty) as family_otf_pos_spend_qty
from
(
    select
     clnt.client_dk
    ,clnt.report_dt
    ,aggr.prd_dc_activity_ind
    ,aggr.prd_cc_activity_ind
    ,aggr.prd_da_activity_ind
    ,aggr.prd_pl_activity_ind
    ,aggr.prd_mg_activity_ind
    ,aggr.prd_client_activity_ind
    ,aggr.prd_ib_activity_1m_ind
    ,aggr.prd_mb_activity_1m_ind
    ,aggr.sd_age_yrs_comp_nv
    ,aggr.lbt_payroll_client_nflag
    ,aggr.lbt_inc_avg_risk_rub_amt
    ,aggr.crd_otf_pos_spend_rub_amt
    ,aggr.crd_otf_pos_spend_qty
    from SBX_RETAIL_DATA.ft_clnt_aggr_mnth as aggr
    inner join family_list_single as clnt
        on aggr.client_dk = clnt.lifestyle_family_dk
        and aggr.report_dt = clnt.report_dt
    where aggr.report_dt =  :v_end_dt
) as m
group by
  m.client_dk
 ,m.report_dt  
;




-- Обновляем основную таблицу данными по средним значениям за месяц
update a
from SBX_RETAIL_SS.ft_ds_features_mnth_3 as a,family_list_rslt as b
set family_qnt = b.family_qnt
	  ,family_dc_activity_ind = b.family_dc_activity_ind
	  ,family_cc_activity_ind = b.family_cc_activity_ind
	  ,family_da_activity_ind = b.family_da_activity_ind
	  ,family_pl_activity_ind = b.family_pl_activity_ind
	  ,family_mg_activity_ind = b.family_mg_activity_ind
	  ,family_client_activity_ind = b.family_client_activity_ind
	  ,family_ib_activity_1m_ind = b.family_ib_activity_1m_ind
	  ,family_mb_activity_1m_ind = b.family_mb_activity_1m_ind
	  ,family_age_yrs_comp_nv = b.family_age_yrs_comp_nv
	  ,family_payroll_client_nflag = b.family_payroll_client_nflag
	  ,family_inc_avg_risk_rub_amt = b.family_inc_avg_risk_rub_amt
	  ,family_otf_pos_spend_rub_amt = b.family_otf_pos_spend_rub_amt
	  ,family_otf_pos_spend_qty = b.family_otf_pos_spend_qty
where a.client_dk = b.client_dk
and a.report_dt = :v_end_dt
;

--- Лог
insert into sbx_retail_ss_core.A19_log_features_mnth_3
select
	'SBX_RETAIL_SS.ft_ds_features_mnth_3'
   ,:step
   ,'Activity_Count: '||Activity_Count
   ,null
   ,current_timestamp(0)
;



drop table sbol_mkb_p2p;
drop table sbol_mkb_p2p_rslt;
drop table main;
drop table main_request;
drop table main_applic;
drop table main_applic_rslt;
drop table TRX_ecom;
drop table vklad;
drop table vklad2;
drop table vklad3;
drop table vklad_rslt;
drop table credits;
drop table bnk_crd;
drop table family_list;
drop table family_list_single;
drop table family_list_rslt;

collect stats column(client_dk) on SBX_RETAIL_SS.ft_ds_features_mnth_3;

end;