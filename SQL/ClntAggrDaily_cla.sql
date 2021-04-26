replace procedure SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla (p_report_dt date) sql Security Creator
/*
--cla
cla_pl2_active_last_dt
cla_mg2_active_last_dt
cla_curr_acc_active_last_dt
cla_dep_active_last_dt 
*/
begin
declare date1 DATE;
declare date2 DATE;
DECLARE vFromDt DATE;
DECLARE sql_code VARCHAR(31000) ;
DECLARE quote_char CHAR(1) DEFAULT ''''; 
declare tm_report_dt date;

------------------------------------------------	
--DECLARE sp_Fill_ft_ClntAggrDaily
------------------------------------------------	
declare msg varchar(255);
declare step int default 0;
declare report_dt_max DATE;
declare date_not_found condition;
declare repBgnDt date;
declare repEndDt date;
declare trnEventStartDt date;

	declare exit handler for sqlexception	
		begin
			get diagnostics exception 1 msg=message_text;
			--- Запись в лог если на каком-то этапе процедура падает
			insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg,var_1,var_1_describe, dtm)
			values
			('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla','SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily',:step,:msg,ACTIVITY_COUNT,'ACTIVITY_COUNT',current_timestamp(0));
			resignal;
		end;

	declare exit handler for date_not_found
		begin
			insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
			values ('SBX_RETAIL_SS.sp_Fill_ft_ClntAggrDaily','SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily',:step,'Previous date is not found. p_report_dt = ' || cast(p_report_dt as varchar(10)),current_timestamp(0));			
		end;

------------------------------------------------	
--FIRST SET
------------------------------------------------	
	SET repEndDt = p_report_dt;          				-- Конец периода 90 дней
	SET repBgnDt = repEndDt - 89;     					-- Начало периода 90 дней
	SET trnEventStartDt = OAdd_Months(repBgnDt, -2);  	-- Начало периода для ограничения выгрузки транзакций
	SET report_dt_max = (SELECT  MAX(report_dt) FROM SBX_Retail_DATA.ft_clnt_aggr_mnth WHERE report_dt >= oadd_months(:repEndDt,-2) and report_dt < :repEndDt);
	SET date1=last_day(OAdd_Months (p_report_dt, -2));
	SET date2=last_day(OAdd_Months (p_report_dt, -1));
	SET vFromDt = report_dt_max + 1; --Trunc(p_report_dt,'MM');

------------------------------------------------	
--client's list
------------------------------------------------	
		--tab_client for all procedure
		set step = 1;
		CALL sbx_retail_ss.sau_DropTable('tab_client'); 
		CREATE MULTISET VOLATILE TABLE tab_client AS (		
		select client_dk 
		from SBX_Retail_Data.dm_client 
		where :p_report_dt between row_actual_from_dt and row_actual_to_dt
		)
		WITH NO DATA
		PRIMARY INDEX(client_dk)
		ON COMMIT PRESERVE ROWS
		;
		INSERT INTO tab_client
		select client_dk 
		from SBX_Retail_Data.dm_client 
		where :p_report_dt between row_actual_from_dt and row_actual_to_dt
		;

		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'tab_client - filling completed',ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
		
		COLLECT STATS COLUMN (client_dk) ON tab_client ;
		
------------------------------------------------	
--tmp_prev
------------------------------------------------
		set step = 2;	
		CALL sbx_retail_ss.sau_DropTable('tmp_prev');	
		CREATE MULTISET VOLATILE TABLE tmp_prev 
			AS
			(
				SELECT a.client_dk 
							,a.report_dt 	
							,a.cla_pl2_active_last_dt
							,a.cla_mg2_active_last_dt 
							,a.cla_dep_active_last_dt	
							,a.cla_curr_acc_active_last_dt
				FROM SBX_Retail_DATA.ft_clnt_aggr_mnth a
							--inner join tab_client b on a.client_dk = b.client_dk
				WHERE a.report_dt = :report_dt_max
			)
		WITH NO DATA
		PRIMARY INDEX ( client_dk )
		ON COMMIT PRESERVE ROWS;
			
		INSERT INTO tmp_prev (client_dk, report_dt 	
								,cla_pl2_active_last_dt
								,cla_mg2_active_last_dt 
								,cla_dep_active_last_dt	
								,cla_curr_acc_active_last_dt
								)
		SELECT a.client_dk 
							,a.report_dt 	
							,a.cla_pl2_active_last_dt
							,a.cla_mg2_active_last_dt 
							,a.cla_dep_active_last_dt	
							,a.cla_curr_acc_active_last_dt
		FROM SBX_Retail_DATA.ft_clnt_aggr_mnth a
					--inner join tab_client b on a.client_dk = b.client_dk
		WHERE a.report_dt = :report_dt_max
		;	
		
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'tmp_prev - filling completed', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
		
		COLLECT STATS COLUMN (client_dk) ON tmp_prev ;

------------------------------------------------	
--sp1 : start SBX_RETAIL_SS.sau_sp_customer_activity
------------------------------------------------	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg,dtm)
	values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla',0, 'repBgnDt: ' || :repBgnDt
		|| '. repEndDt: ' || :repEndDt
		|| '. range: ' || cast ((repEndDt - repBgnDt + 1) as varchar(10)) || '.'
		,current_timestamp(0));

	IF p_report_dt +3 > date THEN
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla',:step, 'Данные для отчета на эту дату еще не доступны' , ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
	ELSE 
		
		--проверка границ интервала
		if (report_dt_max is null)
		then 
			signal date_not_found;
		end if
		;
					
	-----------------------------------------------------------------------
	-- start calc for current month
	-----------------------------------------------------------------------
		
		set step = 3;
		CALL sbx_retail_ss.sau_DropTable('bal_curr');
		CREATE MULTISET VOLATILE TABLE bal_curr AS (
			SELECT loan_agrmnt_id
				, npl_nflag			-- Флаг неработающего кредита (NPL) - просрочка 90+
			FROM SBX_Retail_DATA.ft_loan_agrmnt_bal bal		-- Остаток задолженности и уровень просрочки по кредитному договору на дату.
			WHERE bal.report_dt = :p_report_dt
		) WITH DATA
		PRIMARY INDEX (loan_agrmnt_id)
		ON COMMIT PRESERVE ROWS
		;
		
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'bal_curr - filling completed', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
		
		COLLECT STAT COLUMN(loan_agrmnt_id) ON bal_curr;
		
		set step = 4;
		CALL sbx_retail_ss.sau_DropTable('loans_curr');
		CREATE MULTISET VOLATILE TABLE loans_curr AS (
		SELECT 
		    a.loan_agrmnt_id,
		    a.client_dk, 
		    Coalesce(a.loan_type_cd , CASE loan_class_cd WHEN 'LOAN' THEN 'PL' ELSE loan_class_cd END) AS loan_type_cd,
		    a.agrmnt_open_dt,
		    a.agrmnt_close_dt,      
			lnk.client_dk AS client_dk2,        
			b.npl_nflag,
		    Row_Number()Over(PARTITION BY a.loan_agrmnt_id ORDER BY lnk.client_dk) rnk 
		FROM 
			SBX_Retail_DATA.dm_loan_agrmnt a	-- Кредитный договор ЕКП.
			LEFT JOIN SBX_Retail_DATA.lnk_loan_client lnk	-- Таблица связей созаемщиков с кредитным договором. По одному кредиту может быть несколько созаемщиков. Примечание: связи с основным заемщиком в этой таблице нет.
				ON lnk.loan_agrmnt_id=a.loan_agrmnt_id
				AND :p_report_dt BETWEEN lnk.row_actual_from_dt AND lnk.row_actual_to_dt    
			LEFT JOIN bal_curr b
				ON (a.loan_agrmnt_id = b.loan_agrmnt_id )	
		WHERE 1=1
			AND :p_report_dt BETWEEN a.row_actual_from_dt AND a.row_actual_to_dt
			AND a.client_dk <> -1
		) WITH NO DATA
		PRIMARY INDEX (loan_agrmnt_id)
		ON COMMIT PRESERVE ROWS
		;
		INSERT INTO loans_curr (loan_agrmnt_id, client_dk, loan_type_cd, agrmnt_open_dt, agrmnt_close_dt, client_dk2, npl_nflag, rnk )
		SELECT 
		    a.loan_agrmnt_id,
		    a.client_dk, 
		    Coalesce(a.loan_type_cd , CASE loan_class_cd WHEN 'LOAN' THEN 'PL' ELSE loan_class_cd END) AS loan_type_cd,
		    a.agrmnt_open_dt,
		    a.agrmnt_close_dt,      
			lnk.client_dk AS client_dk2,        
			b.npl_nflag,
		    Row_Number()Over(PARTITION BY a.loan_agrmnt_id ORDER BY lnk.client_dk) rnk 
		FROM 
			SBX_Retail_DATA.dm_loan_agrmnt a	 
			LEFT JOIN SBX_Retail_DATA.lnk_loan_client lnk
				ON lnk.loan_agrmnt_id=a.loan_agrmnt_id
				AND :p_report_dt BETWEEN lnk.row_actual_from_dt AND lnk.row_actual_to_dt    
			LEFT JOIN bal_curr b
				ON (a.loan_agrmnt_id = b.loan_agrmnt_id )	
		WHERE 1=1
			AND :p_report_dt BETWEEN a.row_actual_from_dt AND a.row_actual_to_dt              
			AND a.client_dk <> -1
		;
		
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'loans_curr - filling completed', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
		
		COLLECT STAT COLUMN(loan_agrmnt_id) ON loans_curr;
		
		set step = 5;
		CALL sbx_retail_ss.sau_DropTable('loan_clients_curr');
		CREATE MULTISET VOLATILE TABLE loan_clients_curr AS
		(
		SELECT 
		    client_dk,
		    Max(mg_active_nflag) AS mg_active_nflag,
		    Max(pl_active_nflag) AS pl_active_nflag
		FROM (
		        SELECT 
		            client_dk,
		            Max(CASE 
		                WHEN Coalesce(a.agrmnt_close_dt,DATE'9999-12-31')> :p_report_dt AND a.agrmnt_open_dt <= :p_report_dt AND a.loan_type_cd='MORTGAGE' AND a.rnk=1 THEN 1 
		                ELSE 0
		            END) mg_active_nflag,
		            Max(CASE 
		                WHEN Coalesce(a.agrmnt_close_dt,DATE'9999-12-31')> :p_report_dt AND a.agrmnt_open_dt <= :p_report_dt AND a.loan_type_cd='PL' AND a.rnk=1 THEN 1
		                ELSE 0
		            END) pl_active_nflag
		        FROM loans_curr a
				WHERE Coalesce(npl_nflag,0)=0
		        GROUP BY 1
		        UNION ALL
		        SELECT --созаемщики
		            client_dk2,
		                Max(CASE 
		                WHEN Coalesce(b.agrmnt_close_dt,DATE'9999-12-31')>:p_report_dt AND b.agrmnt_open_dt<=:p_report_dt AND b.loan_type_cd='MORTGAGE' THEN 1 
		                ELSE 0
		            END) mg_active_nflag,
		            0 AS pl_active_nflag
		        FROM loans_curr b    
		        WHERE client_dk2 IS NOT NULL AND Coalesce(npl_nflag,0)=0
		        GROUP BY 1
		    ) t
		    GROUP BY 1
		)   
		WITH NO DATA
		PRIMARY INDEX (client_dk)
		ON COMMIT PRESERVE ROWS;
		
		INSERT INTO loan_clients_curr (client_dk, mg_active_nflag, pl_active_nflag)
		SELECT 
		    client_dk,
		    Max(mg_active_nflag) AS mg_active_nflag,
		    Max(pl_active_nflag) AS pl_active_nflag
		FROM (
		        SELECT 
		            client_dk,
		            Max(CASE 
		                WHEN Coalesce(a.agrmnt_close_dt,DATE'9999-12-31')>:p_report_dt AND a.agrmnt_open_dt<=:p_report_dt AND a.loan_type_cd='MORTGAGE' AND a.rnk=1 THEN 1 
		                ELSE 0
		            END) mg_active_nflag,
		            Max(CASE 
		                WHEN Coalesce(a.agrmnt_close_dt,DATE'9999-12-31')>:p_report_dt AND a.agrmnt_open_dt<=:p_report_dt AND a.loan_type_cd='PL' AND a.rnk=1 THEN 1
		                ELSE 0
		            END) pl_active_nflag
		        FROM loans_curr a
				WHERE Coalesce(npl_nflag,0)=0
		        GROUP BY 1
		        UNION ALL
		        SELECT --созаемщики
		            client_dk2,
		                Max(CASE 
		                WHEN Coalesce(b.agrmnt_close_dt,DATE'9999-12-31')>:p_report_dt AND b.agrmnt_open_dt<=:p_report_dt AND b.loan_type_cd='MORTGAGE' THEN 1 
		                ELSE 0
		            END) mg_active_nflag,
		            0 AS pl_active_nflag
		        FROM loans_curr b    
		        WHERE client_dk2 IS NOT NULL AND Coalesce(npl_nflag,0)=0
		        GROUP BY 1
		    ) t
		    GROUP BY 1
			;
		
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'loan_clients_curr - filling completed', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
		
		COLLECT STATS COLUMN (client_dk) ON loan_clients_curr;
			
		set step = 6;
		CALL sbx_retail_ss.sau_DropTable('tab_15_curr');	
		CREATE MULTISET VOLATILE TABLE tab_15_curr AS (		
		SELECT client_dk, Max(:p_report_dt)  AS p_report_dt
		GROUP BY 1
		FROM loan_clients_curr
		WHERE Coalesce(mg_active_nflag,0)>0 --примечание ppc_mg_active_nflag = mg_active_nflag
		)
		WITH DATA
		PRIMARY INDEX(client_dk)
		ON COMMIT PRESERVE ROWS
		;
		
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'tab_15_curr - filling completed', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
		
		COLLECT STATS COLUMN (client_dk) ON tab_15_curr ;
		

		set step = 7;
		CALL sbx_retail_ss.sau_DropTable('tab_16_curr');	
		CREATE MULTISET VOLATILE TABLE tab_16_curr AS (		
		SELECT client_dk, Max(:p_report_dt)  AS p_report_dt
		GROUP BY 1
		FROM loan_clients_curr
		WHERE Coalesce(pl_active_nflag,0)>0 -- примечание ppc_pl_active_nflag = pl_active_nflag
		)
		WITH DATA
		PRIMARY INDEX(client_dk)
		ON COMMIT PRESERVE ROWS
		;
		
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'tab_16_curr - filling completed', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
		
		COLLECT STATS COLUMN (client_dk) ON tab_16_curr;
		

		set step = 8;
		CALL sbx_retail_ss.sau_DropTable('tab_cur_pl2_mr2');	
		CREATE MULTISET VOLATILE TABLE tab_cur_pl2_mr2 AS (		
		SELECT 	
			tab25.client_dk,
			:p_report_dt AS report_dt,
			tab15.p_report_dt AS cla_mg2_active_last_dt,
			tab16.p_report_dt AS cla_pl2_active_last_dt
		FROM tab_client as tab25 
			LEFT JOIN tab_15_curr as tab15 ON tab25.client_dk=tab15.client_dk
			LEFT JOIN tab_16_curr as tab16 ON tab25.client_dk=tab16.client_dk
		)
		WITH DATA
		PRIMARY INDEX(client_dk)
		ON COMMIT PRESERVE ROWS
		;	
		
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'tab_cur_pl2_mr2 - filling completed', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
		
		COLLECT STATS COLUMN (client_dk) ON tab_cur_pl2_mr2;	
		

		-----------------------------------------------------------------------
		-- end calc for current month
		-----------------------------------------------------------------------

		set step = 9;
		-- collect final data
		CALL sbx_retail_ss.sau_DropTable('tmp_fin_sp1');	
		CREATE MULTISET VOLATILE TABLE tmp_fin_sp1 AS
		(
		SELECT client_dk, 
			 Max(cla_pl2_active_last_dt) AS cla_pl2_active_last_dt,
			 Max(cla_mg2_active_last_dt) AS cla_mg2_active_last_dt
			 GROUP BY 1
		FROM (
			SELECT  client_dk,
				 cla_pl2_active_last_dt,
				 cla_mg2_active_last_dt
			FROM  tab_cur_pl2_mr2
			UNION ALL
			SELECT  client_dk,	
					 cla_pl2_active_last_dt,
				 	cla_mg2_active_last_dt
			FROM tmp_prev) q
		)
		WITH DATA
		PRIMARY INDEX ( client_dk )
		ON COMMIT PRESERVE ROWS;
		
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'tmp_fin_sp1 - filling completed', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));
		

		COLLECT STATS COLUMN (client_dk) ON tmp_fin_sp1;	
		
		
		set step = 10;	-- Заливка в таблицу sau_customer_activity
		
		BEGIN TRANSACTION;

		update SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily
			set   cla_pl2_active_last_dt = NULL
				, cla_mg2_active_last_dt = NULL
				
				, ppc_mg_active_nflag = 0
				, ppc_pl_active_nflag = 0
			where report_dt = :repEndDt;
			
		update SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily
		from (SELECT 
				  tmp_ft_clnt_aggr_daily.report_dt
				, tmp_ft_clnt_aggr_daily.client_dk
				, cla.cla_pl2_active_last_dt
				, cla.cla_mg2_active_last_dt
				,case when cla.cla_pl2_active_last_dt = p_report_dt then 1 else 0 end as ppc_pl_active_nflag
				,case when cla.cla_mg2_active_last_dt = p_report_dt then 1 else 0 end as ppc_mg_active_nflag
			from SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily 
					left join tmp_fin_sp1 cla on tmp_ft_clnt_aggr_daily.client_dk = cla.client_dk
			where report_dt = :repEndDt 
			) as src						
		set	cla_pl2_active_last_dt = src.cla_pl2_active_last_dt
			, cla_mg2_active_last_dt = src.cla_mg2_active_last_dt
			, ppc_pl_active_nflag = src.ppc_pl_active_nflag
			, ppc_mg_active_nflag = src.ppc_mg_active_nflag
		where tmp_ft_clnt_aggr_daily.report_dt = src.report_dt
						and tmp_ft_clnt_aggr_daily.client_dk = src.client_dk 
			;
			
			insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
			values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'Заливка в таблицу sau_customer_activity', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));

		END TRANSACTION;
	
	END IF;	-- Проверка, что данные на p_report_dt уже есть

------------------------------------------------	
--sp1 : end   SBX_RETAIL_SS.sau_sp_customer_activity
------------------------------------------------	
------------------------------------------------	
--sp2 : start SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep
------------------------------------------------	
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
--	declare variables and handler for sql exceptions
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
		
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',-1,'start / p_report_dt = ' || cast(p_report_dt as varchar(10)),current_timestamp(0));

	--проверка границ интервала
	if (report_dt_max is null)
	then 
			signal date_not_found;
	end if
	;
			
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
--	get client_dk and features for the last available month from table SBX_Retail_DATA.ft_clnt_aggr_mnth
--------------------------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------
-- start calc for current month
-----------------------------------------------------------------------
	set step = 11;
	-- Временная таблица с действующими договорами
	CALL SBX_RETAIL_SS.sau_DropTable('vtDepCurrAgr1');
	CREATE MULTISET VOLATILE TABLE vtDepCurrAgr1 AS (
	SELECT
		da.dep_agrmnt_dk,
		da.client_dk AS client_dk,
		da.agrmnt_product_dk,
		da.acct_division_dk,
		da.dep_product_cat_cd,
		da.acct_type_cd, 
		da.agrmnt_open_dt,
		da.agrmnt_close_actual_dt,
		da.dep_chnl_type_rk,
		da.payroll_agrmnt_id
	FROM SBX_RETAIL_DATA.dm_dep_agrmnt da
	WHERE p_report_dt BETWEEN da.row_actual_from_dt AND da.row_actual_to_dt
	AND da.agrmnt_open_dt <= p_report_dt
	)
	WITH DATA
	PRIMARY INDEX(dep_agrmnt_dk)
	ON COMMIT PRESERVE ROWS;
	
	COLLECT STATISTICS USING SAMPLE 2 PERCENT COLUMN(dep_agrmnt_dk) ON vtDepCurrAgr1;
	COLLECT STATISTICS USING SAMPLE 2 PERCENT COLUMN(agrmnt_product_dk) ON vtDepCurrAgr1;
	COLLECT STATISTICS USING SAMPLE 2 PERCENT COLUMN(acct_division_dk) ON vtDepCurrAgr1;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtDepCurrAgr1 - filling completed',current_timestamp(0));		
	
	set step = 12;
	-- 'аблица с продуками	
	CALL SBX_RETAIL_SS.sau_DropTable('vtProduct');
	CREATE VOLATILE TABLE vtProduct AS (
		SELECT *
		FROM SBX_Retail_DATA.ref_product  pr
		WHERE pr.source_system_rk IN 
			(SELECT srs.source_system_rk FROM SBX_RETAIL_DATA.ref_source_system srs WHERE srs.dwh_info_system_type_cd = '007')
		AND p_report_dt BETWEEN pr.row_actual_from_dt AND pr.row_actual_to_dt
	)
	WITH DATA
	UNIQUE PRIMARY INDEX(product_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATISTICS  COLUMN (product_dk) ON vtProduct;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtProduct - filling completed',current_timestamp(0));		
		
	set step = 13;
	-- 'аблица с В'П
	CALL SBX_RETAIL_SS.sau_DropTable('vtDivision');
	CREATE VOLATILE TABLE vtDivision AS (
	SELECT 
		div.*
	FROM SBX_RETAIL_DATA.ref_Division div
	WHERE p_report_dt BETWEEN div.row_actual_from_dt AND div.row_actual_to_dt
	)
	WITH DATA
	UNIQUE PRIMARY INDEX(division_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATISTICS COLUMN (division_dk) ON vtDivision;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtDivision - filling completed',current_timestamp(0));		
	
	set step = 14;
	-- Объединенная таблица с атрибуамт договора, В'П и продукта
	CALL SBX_RETAIL_SS.sau_DropTable('vtDepCurrAgr');
	CREATE MULTISET VOLATILE TABLE vtDepCurrAgr AS (
	SELECT 
		da.*, 
		pr.product_name, 
		div.tb_cd, 
		div.osb_cd, 
		div.vsp_cd
	FROM vtDepCurrAgr1 da
		JOIN  vtProduct pr ON (da.agrmnt_product_dk = pr.product_dk)
		JOIN  vtDivision div ON (da.acct_division_dk = div.division_dk)
	)
	WITH DATA
	PRIMARY INDEX(dep_agrmnt_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATISTICS USING SAMPLE 2 PERCENT COLUMN (dep_agrmnt_dk) ON vtDepCurrAgr;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtDepCurrAgr - filling completed',current_timestamp(0));		
	
	set step = 15;
	CALL SBX_RETAIL_SS.sau_DropTable('vtDepAggrCurr');
	CREATE MULTISET VOLATILE TABLE vtDepAggrCurr AS (
	SELECT
		da.acct_dwh_id AS coa_id,
		da.dep_agrmnt_dk
	FROM SBX_Retail_DATA.dm_dep_agrmnt da
	WHERE p_report_dt BETWEEN da.row_actual_from_dt AND da.row_actual_to_dt
	QUALIFY Row_Number() Over (PARTITION BY da.acct_dwh_id ORDER BY da.row_actual_from_dt DESC) = 1)
	WITH DATA
	PRIMARY INDEX (coa_id)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATISTICS USING SAMPLE 2 PERCENT COLUMN (coa_id) ON vtDepAggrCurr;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtDepCurrAgr - filling completed',current_timestamp(0));		

	set step = 16;
	CALL SBX_RETAIL_SS.sau_DropTable('tmp_ft_dep_txn_aggr');
	CREATE MULTISET VOLATILE TABLE tmp_ft_dep_txn_aggr AS (
	SELECT
			td.dep_agrmnt_dk,
			Coalesce(coa_id,-1) AS coa_dwh_id,
			Coalesce(client_dk,-1) AS client_dk,
			Coalesce(epk_id,-1) AS epk_id,
			p_report_dt  as report_dt,
			txn_oper_acct_type_cd AS txn_deb_cred_cd,
			Coalesce(txn_cod_type_rk,-1) AS txn_cod_type_rk,
			Coalesce(txn_pmt_reg_type_rk,-1) AS txn_pmt_reg_type_rk,
			txn_ccy AS txn_ccy_cd,
			Coalesce(division_dk,-1) AS txn_division_dk,
			Coalesce(txn_chnl_type_rk,-1) AS txn_chnl_type_rk,
			Sum(txn_ccy_amt) AS txn_ccy_amt,
			Sum(txn_rub_amt) AS txn_rub_amt,
			Count(1) AS txn_qty,
			Min(txn_dt) AS txn_min_dt,		
			Max(txn_dt) AS txn_max_dt,
			Max(txn_ccy_amt) AS	txn_max_ccy_amt,		
			Max(txn_rub_amt) AS txn_max_rub_amt
	FROM SBX_Retail_DATA.ft_dep_agrmnt_txn_detail td 
		LEFT JOIN vtDepAggrCurr dac ON dac.dep_agrmnt_dk=td.dep_agrmnt_dk
	WHERE txn_dt BETWEEN vFromDt AND p_report_dt	AND td.dep_agrmnt_dk>0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    )
    WITH DATA
    PRIMARY INDEX(dep_agrmnt_dk)
    ON COMMIT PRESERVE ROWS;
	COLLECT STATISTICS USING SAMPLE 2 PERCENT COLUMN (dep_agrmnt_dk) ON tmp_ft_dep_txn_aggr;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'tmp_ft_dep_txn_aggr - filling completed',current_timestamp(0));		
	
	set step = 17;
	CALL SBX_RETAIL_SS.sau_DropTable('vtAgrmntTxnAggr');
	CREATE MULTISET VOLATILE TABLE vtAgrmntTxnAggr AS (  
    SELECT 
		dat.dep_agrmnt_dk
		,Sum(CASE WHEN dat.txn_deb_cred_cd = 'C' THEN dat.txn_qty END) AS txn_inf_total_qty
		,Sum(CASE WHEN dat.txn_deb_cred_cd = 'D' AND dat.txn_cod_type_rk = 2 THEN dat.txn_qty END) AS txn_otf_intr_pmt_qty
		,Sum(CASE WHEN dat.txn_deb_cred_cd = 'D' THEN dat.txn_qty END) AS txn_otf_total_qty
		,Sum(CASE WHEN dat.txn_deb_cred_cd = 'C' AND dat.txn_cod_type_rk  IN (11,14,22) THEN dat.txn_qty END) AS txn_inf_intr_cap_qty
		,Sum(CASE WHEN dat.txn_deb_cred_cd = 'D' AND dat.txn_cod_type_rk = 10 THEN dat.txn_qty END) AS txn_otf_acct_clsr_qty
		,Sum(CASE WHEN dat.txn_deb_cred_cd = 'C' AND (dat.txn_cod_type_rk  NOT IN (21,20,8,7,11,14,22) OR (dat.txn_cod_type_rk = 21 AND txn_pmt_reg_type_rk = 6)) THEN dat.txn_qty END) AS txn_inf_othr_qty
		,Sum(CASE WHEN dat.txn_deb_cred_cd = 'C' AND (dat.txn_cod_type_rk  NOT IN (21,20,8,7,11,14,22) OR (dat.txn_cod_type_rk = 21 AND txn_pmt_reg_type_rk = 6)) THEN dat.txn_rub_amt END) AS txn_inf_othr_rub_amt
	FROM tmp_ft_dep_txn_aggr dat
    WHERE dat.report_dt = p_report_dt
    GROUP BY 1
    )
    WITH DATA
    PRIMARY INDEX(dep_agrmnt_dk)
    ON COMMIT PRESERVE ROWS;
	COLLECT STATISTICS USING SAMPLE 2 PERCENT COLUMN (dep_agrmnt_dk) ON vtAgrmntTxnAggr;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtAgrmntTxnAggr - filling completed',current_timestamp(0));		
	
			
	set step = 18;
	CALL SBX_RETAIL_SS.sau_DropTable('vtDepBalCurr');

	CREATE MULTISET VOLATILE TABLE vtDepBalCurr AS 

    (
    SELECT das.dep_agrmnt_dk
			,dep_curr_bal_rub_amt
			,report_dt
    FROM SBX_Retail_DATA.ft_dep_agrmnt_state das
    WHERE das.report_dt = p_report_dt
    )
	WITH DATA
	PRIMARY INDEX(dep_agrmnt_dk)
	ON COMMIT PRESERVE ROWS;
	COLLECT STATISTICS USING SAMPLE 2 PERCENT COLUMN (dep_agrmnt_dk) ON vtDepBalCurr;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtDepBalCurr - filling completed',current_timestamp(0));		
	
	set step = 19;
	-- "инансовые показатели за отчетный месяц
	CALL SBX_RETAIL_SS.sau_DropTable('vtCurrBal1');
	CREATE MULTISET VOLATILE TABLE vtCurrBal1 AS (
	SELECT
		 das.dep_agrmnt_dk
		,das.dep_curr_bal_rub_amt

		,txn_inf_total_qty
		,txn_otf_intr_pmt_qty
		,txn_otf_total_qty
		,txn_inf_intr_cap_qty
		,txn_otf_acct_clsr_qty
		,txn_inf_othr_qty
		,txn_inf_othr_rub_amt

	FROM vtDepBalCurr das
		LEFT JOIN vtAgrmntTxnAggr cur ON (das.dep_agrmnt_dk = cur.dep_agrmnt_dk)
	WHERE das.report_dt = p_report_dt
	)
	WITH DATA
	PRIMARY INDEX(dep_agrmnt_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATISTICS  USING SAMPLE 2 PERCENT COLUMN(dep_agrmnt_dk) ON vtCurrBal1;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtCurrBal1 - filling completed',current_timestamp(0));		
	
	set step = 20;
	CALL SBX_RETAIL_SS.sau_DropTable('vtCurrBal');
	CREATE MULTISET VOLATILE TABLE vtCurrBal AS (
	SELECT
		bal1.dep_agrmnt_dk
		,dep_curr_bal_rub_amt
		,txn_inf_total_qty
		,txn_otf_intr_pmt_qty
		,txn_otf_total_qty
		,txn_inf_intr_cap_qty
		,txn_otf_acct_clsr_qty
		,txn_inf_othr_qty
		,txn_inf_othr_rub_amt
	FROM vtCurrBal1 bal1
	--LEFT JOIN  vt12MAggr bal4 ON (bal1.dep_agrmnt_dk = bal4.dep_agrmnt_dk)
	)
	WITH DATA
	PRIMARY INDEX(dep_agrmnt_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATISTICS  USING SAMPLE 2 PERCENT COLUMN(dep_agrmnt_dk) ON vtCurrBal;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtCurrBal - filling completed',current_timestamp(0));		

	set step = 21;
	CALL SBX_RETAIL_SS.sau_DropTable('vtAgrBalFull');
	CREATE MULTISET VOLATILE TABLE vtAgrBalFull AS (
	SELECT
		da.*, 
		dep_curr_bal_rub_amt
		
		,CASE 
			WHEN da.dep_product_cat_cd='CURRENT_ACCOUNT' 
				AND (Coalesce(txn_inf_total_qty,0) 
						- Coalesce(txn_otf_intr_pmt_qty,0) 
						+ Coalesce(txn_otf_total_qty,0) 
						- Coalesce(txn_inf_intr_cap_qty,0) 
						- Coalesce(txn_otf_acct_clsr_qty,0)
						- Coalesce(txn_inf_othr_qty,0)*(CASE 
															WHEN txn_inf_othr_rub_amt>0 THEN 0 
															ELSE 1 
														end)	
					)>0 
				THEN 1 
			ELSE 0 
		END AS curr_acc_activity_nflag
		
	FROM    		vtCurrBal AS daa
	RIGHT JOIN 	vtDepCurrAgr AS da ON (daa.dep_agrmnt_dk = da.dep_agrmnt_dk)
	WHERE client_dk <> -1
	)
	WITH DATA
	PRIMARY INDEX(client_dk)
	ON COMMIT PRESERVE ROWS
	;
		COLLECT STATISTICS  USING SAMPLE 2 PERCENT COLUMN(client_dk) ON vtAgrBalFull;
		
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtAgrBalFull - filling completed',current_timestamp(0));		
	

	set step = 22;
	CALL SBX_RETAIL_SS.sau_DropTable('vtCurrStateTxn');
	CREATE MULTISET VOLATILE TABLE vtCurrStateTxn AS 
	(
	SELECT
			client_dk AS client_dk
			,Sum(CASE WHEN dep_product_cat_cd = 'TERM_DEPOSIT' THEN dep_curr_bal_rub_amt END) AS lbt_acct_dep_td_bal_rub_amt
			,Sum(CASE WHEN dep_product_cat_cd = 'CURRENT_ACCOUNT' THEN dep_curr_bal_rub_amt END) AS lbt_acct_dep_ca_bal_rub_amt
			,Max(curr_acc_activity_nflag) AS lbt_curr_acc_activity_nflag
	FROM 		
			   vtAgrBalFull
	GROUP BY 1
	) 
	WITH DATA
	PRIMARY INDEX (client_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATISTICS USING SAMPLE 2 PERCENT COLUMN(client_dk) ON vtCurrStateTxn;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtCurrStateTxn - filling completed',current_timestamp(0));
	
	set step = 23;
	-- "инальная таблица
	CALL SBX_RETAIL_SS.sau_DropTable('vtDepFinal');
	CREATE MULTISET VOLATILE TABLE vtDepFinal AS (	
			SELECT
				mn.client_dk, 
				mn.lbt_acct_dep_td_bal_rub_amt,
				mn.lbt_acct_dep_ca_bal_rub_amt,
				mn.lbt_curr_acc_activity_nflag
			FROM vtCurrStateTxn AS mn
				--JOIN vtAcctFirstLast AS afl ON  afl.client_dk = mn.client_dk
	)
	WITH DATA
	PRIMARY INDEX (client_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATISTICS COLUMN(client_dk) ON vtDepFinal;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'vtDepFinal - filling completed',current_timestamp(0));

	set step = 24;
	CALL SBX_RETAIL_SS.sau_DropTable('tab17');	
	CREATE MULTISET VOLATILE TABLE tab17 AS (		
	SEL tab25.client_dk, Max(p_report_dt)  AS p_report_dt
	GROUP BY 1
	FROM tab_client as tab25 
				inner join vtDepFinal on tab25.client_dk = vtDepFinal.client_dk
	--FROM SBX_RETAIL_SS.jc_tmp_clnt_aggr_mnth
	WHERE lbt_acct_dep_td_bal_rub_amt>=1000 
			--AND report_dt=p_report_dt
	)
	WITH DATA
	PRIMARY INDEX(client_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATS COLUMN (client_dk) ON tab17 ;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'tab17 - filling completed',current_timestamp(0));

	set step = 25;
	CALL SBX_RETAIL_SS.sau_DropTable('bal_prev');	
	CREATE SET VOLATILE TABLE bal_prev 
	AS
	(
	SEL client_dk
	FROM SBX_Retail_DATA.ft_clnt_aggr_mnth 
	WHERE report_dt BETWEEN date1 AND date2
			AND lbt_acct_dep_ca_bal_rub_amt >=5000
	)
	WITH DATA
	PRIMARY INDEX ( client_dk )
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATS COLUMN (client_dk) ON bal_prev ;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'bal_prev - filling completed',current_timestamp(0));
	
	
	set step = 26;
	CALL SBX_RETAIL_SS.sau_DropTable('tmp_aggr_mnth');	
	CREATE MULTISET VOLATILE TABLE tmp_aggr_mnth AS 
	(		
			SEL client_dk,  lbt_curr_acc_activity_nflag
			FROM SBX_Retail_DATA.ft_clnt_aggr_mnth a
			WHERE a.report_dt BETWEEN date1 AND date2 
	)
	WITH DATA
	PRIMARY INDEX(client_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATS COLUMN (client_dk) ON tmp_aggr_mnth ;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'tmp_aggr_mnth - filling completed',current_timestamp(0));
	
	set step = 27;
	CALL SBX_RETAIL_SS.sau_DropTable('tmp_curr_acc');	
	CREATE MULTISET VOLATILE TABLE tmp_curr_acc AS 
	(		
		SEL client_dk, Max(lbt_curr_acc_activity_nflag) AS ppc_curr_acc_activity_nflag
		FROM (
			SEL client_dk,  lbt_curr_acc_activity_nflag
			FROM vtDepFinal
			--WHERE report_dt=p_report_dt
			UNION ALL 
			SEL client_dk,  lbt_curr_acc_activity_nflag
			FROM tmp_aggr_mnth
		)q GROUP BY 1
	)
	WITH NO DATA
	PRIMARY INDEX(client_dk)
	ON COMMIT PRESERVE ROWS
	;
	INSERT INTO tmp_curr_acc
	SEL client_dk, Max(lbt_curr_acc_activity_nflag) AS ppc_curr_acc_activity_nflag
		FROM (
			SEL client_dk,  lbt_curr_acc_activity_nflag
			FROM vtDepFinal
			--WHERE report_dt=p_report_dt
			UNION ALL 
			SEL client_dk,  lbt_curr_acc_activity_nflag
			FROM tmp_aggr_mnth
		)q GROUP BY 1
		;	
	COLLECT STATS COLUMN (client_dk) ON tmp_curr_acc ;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'tmp_curr_acc - filling completed',current_timestamp(0));

	set step = 28;
	CALL SBX_RETAIL_SS.sau_DropTable('tab18');	
	CREATE MULTISET VOLATILE TABLE tab18 AS (		
	SEL tab25.client_dk, Max(p_report_dt)  AS p_report_dt
	GROUP BY 1
	--FROM SBX_RETAIL_SS.jc_tmp_clnt_aggr_mnth a 
	FROM tab_client as tab25 
				inner join tmp_curr_acc a on tab25.client_dk = a.client_dk
				inner join vtDepFinal on tab25.client_dk = vtDepFinal.client_dk
				left join bal_prev ON bal_prev.client_dk=tab25.client_dk
	WHERE (ppc_curr_acc_activity_nflag>0 
			OR lbt_acct_dep_ca_bal_rub_amt >=5000 
			OR bal_prev.client_dk IS NOT NULL)  
			--AND report_dt=p_report_dt
	)
	WITH DATA
	PRIMARY INDEX(client_dk)
	ON COMMIT PRESERVE ROWS
	;
	COLLECT STATS COLUMN (client_dk) ON tab18 ;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'tab18 - filling completed',current_timestamp(0));
    
	----------------------------------------------------------------------------------------------------------------------------
	-- union prev and curr data
	----------------------------------------------------------------------------------------------------------------------------
	set step = 29;
	CALL SBX_RETAIL_SS.sau_DropTable('tab_cur_dep_acc');	
	CREATE MULTISET VOLATILE TABLE tab_cur_dep_acc AS (		
	SEL 	
		tab25.client_dk
		,p_report_dt AS report_dt
		,tab17.p_report_dt AS cla_dep_active_last_dt
		,tab18.p_report_dt AS cla_curr_acc_active_last_dt
	FROM tab_client as tab25 
		LEFT JOIN tab17 ON tab25.client_dk=tab17.client_dk	
		LEFT JOIN tab18 ON tab25.client_dk=tab18.client_dk	
	)
	WITH DATA
	PRIMARY INDEX(client_dk)
	ON COMMIT PRESERVE ROWS
	;	
	COLLECT STATS COLUMN (client_dk) ON tab_cur_dep_acc ;	
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'tab_cur_dep_acc - filling completed',current_timestamp(0));

	-----------------------------------------------------------------------
	-- end calc for current month
	-----------------------------------------------------------------------

	set step = 30;
	-- collect final data
	CALL SBX_RETAIL_SS.sau_DropTable('tmp_fin_sp2');	
	CREATE MULTISET VOLATILE TABLE tmp_fin_sp2 AS
	(
	SELECT client_dk		 
		 ,Max(cla_dep_active_last_dt) AS cla_dep_active_last_dt
		 ,Max(cla_curr_acc_active_last_dt) AS cla_curr_acc_active_last_dt
		 GROUP BY 1
	FROM (
		SELECT  client_dk
			,cla_curr_acc_active_last_dt
			,cla_dep_active_last_dt
		FROM  tab_cur_dep_acc
		UNION ALL
		SELECT  client_dk	
			,cla_curr_acc_active_last_dt
			,cla_dep_active_last_dt
		FROM tmp_prev) q
	)
	WITH DATA
	PRIMARY INDEX ( client_dk )
	ON COMMIT PRESERVE ROWS;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.kaa_ft_clnt_aggr_daily_part2',:step,'tmp_fin_sp2 - filling completed',current_timestamp(0));

	set step = 31;	
	
	--UPDATE final data:	
	BEGIN TRANSACTION;

		update SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily
			set   cla_curr_acc_active_last_dt = NULL
				, cla_dep_active_last_dt = NULL
				, dep_activity_nflag = 0
				, lbt_acct_dep_ca_bal_rub_nflag = 0
				, ppc_curr_acc_activity_nflag = 0
			where report_dt = :repEndDt;
			
		update SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily
		from (select 
				  tmp_ft_clnt_aggr_daily.report_dt
				, tmp_ft_clnt_aggr_daily.client_dk
				, cla.cla_curr_acc_active_last_dt
				, cla.cla_dep_active_last_dt
				--флаг наличия суммарного баланс по всем срочным вкладам клиента на отчетную дату в рублях >= 1000 р.
				, case when cla.cla_dep_active_last_dt = p_report_dt then 1 else 0 end as dep_activity_nflag
			from SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily 
					left join tmp_fin_sp2 cla on tmp_ft_clnt_aggr_daily.client_dk = cla.client_dk
			where tmp_ft_clnt_aggr_daily.report_dt = :repEndDt 
			) as src						
		set	cla_curr_acc_active_last_dt = src.cla_curr_acc_active_last_dt
			, cla_dep_active_last_dt = src.cla_dep_active_last_dt
			, dep_activity_nflag = src.dep_activity_nflag
		where tmp_ft_clnt_aggr_daily.report_dt = src.report_dt
						and tmp_ft_clnt_aggr_daily.client_dk = src.client_dk 
			;
		
		--флаг наличия баланса >= 5000 р. на конец периода 
		update a
		from SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily a, vtDepFinal b 
			set lbt_acct_dep_ca_bal_rub_nflag = case 
													when coalesce(b.lbt_acct_dep_ca_bal_rub_amt,0) >=5000 then 1 
													else 0
												end
		where a.client_dk = b.client_dk										
		;
		
		-- флаг наличия транзакции ppc_curr_acc_activity_nflag
		update a
		from SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily a, tmp_curr_acc b 
			set ppc_curr_acc_activity_nflag = coalesce(b.ppc_curr_acc_activity_nflag,0)
		where a.client_dk = b.client_dk			
		;
		
		insert into sbx_retail_ss.kaa_execution_log(proc_name, step, msg, var_1,var_1_describe,dtm)
		values ('SBX_RETAIL_SS.sp_Fill_tmp_ft_ClntAggrDaily_cla', :step, 'Заливка в таблицу sau_customer_activity', ACTIVITY_COUNT, 'ACTIVITY_COUNT', current_timestamp(0));

		END TRANSACTION;
	
	insert into sbx_retail_ss.kaa_execution_log(proc_name, target_table, step, msg, dtm)
	values
	('SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep','SBX_RETAIL_SS.tmp_ft_clnt_aggr_daily',:step,'Data is inserted. Procedure completed',current_timestamp(0));

------------------------------------------------	
--sp2 : end   SBX_RETAIL_SS.kaa_sp_Fill_ClntAggrDaily_cla_curr_acc_dep
------------------------------------------------	

end;