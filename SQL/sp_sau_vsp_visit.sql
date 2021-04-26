

/*
		����������
35      -- ����: ������ �� ���  
392     -- ����: ������ ������ �� ������� ���.	
897     -- ����: �������� ���� 
1054    -- ����: ���������� ���� � ��� 
1186    -- ����: ��������� �������� �� ���� �� 
1371    -- ����: ���������� ������ �����
1400    -- ����: ������ ������ ����
1956    -- ����: ������ ���� - ���
1978    -- ����: ������ CRM 
2082    -- ����: ���������� CRM � ��� 
2185    -- ����: ������ ������ CRM
2218    -- ����: ������ CRM ��� ����
2246    -- ����: ����������� ������� ���� � CRM 
2273    -- ����: ���� ��������� �������
*/

replace PROCEDURE sbx_retail_ss.sp_sau_vsp_visit(rep_dt date)  
SQL SECURITY INVOKER
begin

declare err_msg varchar(255);
declare log_msg varchar(255);
declare step integer;
declare v_proc_id bigint;

declare v_char_dt varchar(20);
declare sql_code varchar(31000) DEFAULT ''; 

	declare exit handler for sqlexception
		begin
			get diagnostics exception 1 err_msg=message_text;
			--- ������ � ��� ���� �� �����-�� ����� ��������� ������
			insert into SBX_RETAIL_SS.sau_test_log(target_table, step, err_msg, cnt_rows, dtm)
			values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ',:step, :err_msg, ACTIVITY_COUNT, current_timestamp(0));
			resignal;
		end;

	set step = 0;
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, '�����. (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	
	set v_proc_id = (select session);
	set v_char_dt = rep_dt (format 'yyyy-mm-dd') (char(10));
	
		
	
/*****************************************************************************************************************/

-- ����: ������ �� ���

/*****************************************************************************************************************/

		
	set step = 1001;
	
	-- ���� � ��������� ���������, ������ �� ��
	CALL sbx_retail_ss.sau_DropTable('vt_posgr');
	SET sql_code = 
	'CREATE MULTISET VOLATILE TABLE vt_posgr AS (
	    select 
			urf_code, 
			EtalonPost,
			POSGR,
			fblock_name,
			Tab_num
	    from foreign table (
		    select 
				us.urf_code,
				us.EtalonPost,
				us.POSGR,
				us.fblock_name,
				us.Tab_num		
			from "001_mis_retail_channel".v_rost_saphr as us
			-- � ���� ����������� ��� ������ ����
			where us.Date_report = (
				select max(Date_report) 
				from "001_mis_retail_channel".v_rost_saphr	-- ���� �������������
				where 0=0
					and Date_report between add_months(date''' || v_char_dt || ''', -1) and date''' || v_char_dt|| '''
			)
	   )@promtd t
	)
	WITH DATA
	PRIMARY INDEX(Tab_num)
	ON COMMIT PRESERVE ROWS'
	;                              
	EXECUTE IMMEDIATE sql_code;
	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_posgr (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));

	
	set step = 1002;
	
	CALL sbx_retail_ss.DropTable('vt_suo_00');
	SET sql_code = 
	'CREATE MULTISET VOLATILE TABLE vt_suo_00 AS (
	    select 
			SUBBRANCH_ID 
			, EVENT_TYPE_ID 
			, EVENT_DTTM 
			, TICKET_ID 
			, CURR_OPCAT_ID 
			, CURR_COUNTER 
			, USER_ID
		    , ORDER_NUM 
			, BUSINESS_DT 
			, TICKET_WAIT_TIME 
			, TICKET_SERV_TIME 
			, ticket_delay_flg 
		    , saphr_id 
			, urf_code_actual 
			, ticket_id_suffix
		    , ticket_tema
	    from foreign table (

			select
				s.SUBBRANCH_ID 
				, s.EVENT_TYPE_ID 
				, s.EVENT_DTTM 
				, s.TICKET_ID 
				, s.CURR_OPCAT_ID 
				, s.CURR_COUNTER 
				, s.USER_ID
			    , s.ORDER_NUM 
				, s.BUSINESS_DT 
				, s.TICKET_WAIT_TIME 
				, s.TICKET_SERV_TIME 
				, s.ticket_delay_flg 
			    , s.saphr_id 
				, s.urf_code_actual 
				, s.ticket_id_suffix
			    , ts.OPCAT_NAME 			as ticket_tema
			from (-- "001_MIS_RETAIL_CHANNEL".VSP_SUO_FCT_EVENTS 
				SELECT
					SUBBRANCH_ID, EVENT_TYPE_ID, EVENT_DTTM, TICKET_ID, CURR_OPCAT_ID, CURR_COUNTER, USER_ID, CUSTOM_PARAM_1, CUSTOM_PARAM_2,
					CUSTOM_PARAM_3, RECEIVE_DTTM, LOAD_ID, ORDER_NUM, BUSINESS_DT, TICKET_WAIT_TIME, TICKET_SERV_TIME, TICKET_REG_FLG, ticket_transfer_cat_flg,
					TICKET_SERV_FLG, ticket_serv_flg_kpi, ticket_transfer_o_flg, ticket_transfer_c_flg, ticket_delay_flg, TICKET_DROP_FLG, bad_user_by_ticket_cnt, bad_user_by_ticket_flg,
					TICKET_SERV_FLG_TECH, saphr_id, urf_code_actual, is_short, TICKET_SERV_FLG_time, role_id, ticket_2nd_reg_flg, ticket_id_suffix
				FROM "001_MIS_RETAIL_CHANNEL".VSP_SUO_FCT_EVENTS_CURRENT
				WHERE
					business_dt >= date''2020-02-01''
				 ) as s
			left join 
					-- "001_MIS_RETAIL_CHANNEL".vsp_oe_smo_isu_excluded_urfs as exclude	
					"001_MIS_Retail_Channel"."vsp_oe_smo_isu_excluded_urfs" as exclude	
				on s.urf_code_actual = exclude.urf_code_actual 
				AND s.business_dt between exclude.per_start and exclude.per_fin 
				AND exclude.cause_short in (''GR'')
			-----���� ������ 
			left join 
					-- "001_MIS_RETAIL_channel".VSP_buf_SUO_OPCAT_HIST ts 
					"001_MIS_Retail_Channel"."VSP_buf_SUO_OPCAT_HIST" ts
				on ts.OPCAT_ID = s.CURR_OPCAT_ID 
				and EFFECTIVE_TO_DTTM = date''5999-12-31''
			where 
				exclude.urf_code_actual is null   	 -- ������ ���������� ���
				and s.business_dt = date''' || v_char_dt || ''' 
				and s.event_type_id in (1,2,3,4,5,6,7) -- ������� ������ ����������

	   	)@promtd t
	)
	WITH DATA
	PRIMARY INDEX(saphr_id)
	ON COMMIT PRESERVE ROWS'
	;                              
	EXECUTE IMMEDIATE sql_code;
	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_suo_00 (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));


	
	set step = 1;
	
	CALL sbx_retail_ss.sau_DropTable('vt_suo_0');
	CREATE MULTISET VOLATILE TABLE vt_suo_0 (
		SUBBRANCH_ID BIGINT,
		EVENT_TYPE_ID BYTEINT,
		EVENT_DTTM TIMESTAMP(6),
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		CURR_OPCAT_ID SMALLINT,
		CURR_COUNTER SMALLINT,
		USER_ID BIGINT,
		ORDER_NUM INTEGER,
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		TICKET_WAIT_TIME INTEGER,
		TICKET_SERV_TIME INTEGER,
		ticket_delay_flg BYTEINT,
		saphr_id INTEGER,
		urf_code_actual VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		ticket_tema VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		POSGR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		fblock_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		EtalonPost VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		urf_code VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC)
	PRIMARY INDEX ( saphr_id )
	PARTITION BY RANGE_N(BUSINESS_DT  BETWEEN DATE '2013-01-01' AND DATE '2021-12-31' EACH INTERVAL '1' DAY ,
	 NO RANGE, UNKNOWN)
	ON COMMIT PRESERVE ROWS;
	 

	insert into vt_suo_0
	select
		suo.SUBBRANCH_ID 
		, suo.EVENT_TYPE_ID 
		, suo.EVENT_DTTM 
		, suo.TICKET_ID 
		, suo.CURR_OPCAT_ID 
		, suo.CURR_COUNTER 
		, suo.USER_ID
	    , suo.ORDER_NUM 
		, suo.BUSINESS_DT 
		, suo.TICKET_WAIT_TIME 
		, suo.TICKET_SERV_TIME 
		, suo.ticket_delay_flg 
	    , suo.saphr_id 
		, suo.urf_code_actual 
		, suo.ticket_id_suffix
	    , suo.ticket_tema
		, posgr.POSGR
		, posgr.fblock_name
		, posgr.EtalonPost
		, posgr.urf_code
	from vt_suo_00 suo
	left join vt_posgr posgr
		on posgr.Tab_num = cast(suo.saphr_id as varchar(50))
	;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_suo_0 (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));

	collect stats column(saphr_id) on vt_suo_0;


	---- ������
	-- 1.2)
	set step = 2;
	CALL sbx_retail_ss.sau_DropTable('vt_SUO_1');
	CREATE MULTISET VOLATILE TABLE vt_SUO_1 (
		SUBBRANCH_ID BIGINT,
		EVENT_TYPE_ID BYTEINT,
		EVENT_DTTM TIMESTAMP(6),
		saphr_id INTEGER,
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		CURR_OPCAT_ID SMALLINT,
		CURR_COUNTER SMALLINT,
		ORDER_NUM INTEGER,
		TICKET_WAIT_TIME INTEGER,
		TICKET_SERV_TIME INTEGER,
		ticket_delay_flg BYTEINT,
		urf_code_actual VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		ticket_tema VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		time_suo_start TIME(6),
		time_suo_end TIME(6),
		POSGR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		fblock_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		EtalonPost VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		urf_code VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC)
	PRIMARY INDEX ( saphr_id )
	ON COMMIT PRESERVE ROWS;
	
	insert into vt_SUO_1
	select 
		su.SUBBRANCH_ID,
		su.EVENT_TYPE_ID,
		su.EVENT_DTTM,
		su.saphr_id,		
		su.BUSINESS_DT,
		su.TICKET_ID,
		su.CURR_OPCAT_ID,
		su.CURR_COUNTER,
		su.ORDER_NUM,
		su.TICKET_WAIT_TIME,
		su.TICKET_SERV_TIME,
		su.ticket_delay_flg,
		su.urf_code_actual,
		su.ticket_id_suffix,
        su.ticket_tema,
		su.time_suo_start, 
	    su.time_suo_end,
		su.POSGR,
		su.fblock_name,
		su.EtalonPost,
		su.urf_code
	from
	    (
	    select
	        s.SUBBRANCH_ID,
			s.EVENT_TYPE_ID,
			s.EVENT_DTTM,
			s.saphr_id,		
			s.BUSINESS_DT,
			s.TICKET_ID,
			s.CURR_OPCAT_ID,
			s.CURR_COUNTER,
			s.ORDER_NUM,
			s.TICKET_WAIT_TIME,
			s.TICKET_SERV_TIME,
			s.ticket_delay_flg,
			s.urf_code_actual,
			s.ticket_id_suffix,
	        s.ticket_tema
			/*, (first_value(s.event_dttm) over (partition by s.urf_code_actual, s.ticket_id, s.ticket_id_suffix  
						order by s.urf_code_actual, s.user_id, s.ticket_id,s.event_dttm 
						rows between 1 preceding and 1 following))(time) 		as time_suo_start*/
	        -- , s.event_dttm(time) 												as time_suo_end
			, min(s.event_dttm) over (partition by s.urf_code_actual, s.ticket_id, s.ticket_id_suffix)(time) 	as time_suo_start	
			, max(s.event_dttm) over (partition by s.urf_code_actual, s.ticket_id, s.ticket_id_suffix)(time)	as time_suo_end
			-- , s.event_dttm(time) 												as time_suo_end
			, s.POSGR
			, s.fblock_name
			, s.EtalonPost
			, s.urf_code
	    from vt_suo_0 as s   
	    where
	        event_type_id > 1  AND 			-- ������� ������ ����������
	        s.event_dttm(time) < '23:59:59' -- ������� ����������� ����� ������� � �������
	    ) as su
	where su.event_type_id in (3,4,5,7) -- ��������� ������ ������, ������� �������� � ���������� ������������. ���������� ������ �������
	;
	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'suo_1. (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	
	collect stats column(saphr_id) on vt_SUO_1;


	-- 1.3)
	/*
	��������� ���� ������������� "001_mis_retail_channel".v_rost_saphr,  
	��������� �� saphr_id � tab_num � �� ���� ������ ����������� (Date_report) � ����� ������ (BUSINESS_DT),  
	��������� �� ���� us.posgr in ('(�)��', '(�)�� ��', '��', '�����', '����', '�����', '��')  
	������� ��� � ���� ��� ����� urf_code is not null.  
	��������� ���������� ��� ������� "001_MIS_RETAIL_channel".VSP_buf_SUO_OPCAT_HIST,  
	��������� �� (OPCAT_ID) � (CURR_OPCAT_ID)�� ������� ���, ��� ���� ��������� �������� ������ ����������� EFFECTIVE_TO_DTTM =date'5999-12-31'
	������� ������������ ���� (OPCAT_NAME as ticket_tema)  
	�������� SUO_1  
	*/

	-- ������������� ���
	set step = 3;
	CALL sbx_retail_ss.sau_DropTable('SUO_1');
	CREATE MULTISET VOLATILE TABLE SUO_1 (
		POSGR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		fblock_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		EtalonPost VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		urf_code VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		SUBBRANCH_ID BIGINT,
		EVENT_TYPE_ID BYTEINT,
		EVENT_DTTM TIMESTAMP(6),
		saphr_id INTEGER,
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		CURR_OPCAT_ID SMALLINT,
		CURR_COUNTER SMALLINT,
		ORDER_NUM INTEGER,
		TICKET_WAIT_TIME INTEGER,
		TICKET_SERV_TIME INTEGER,
		ticket_delay_flg BYTEINT,
		urf_code_actual VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		ticket_tema VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		time_suo_start TIME(6),
		time_suo_end TIME(6))
	PRIMARY INDEX ( saphr_id )
	PARTITION BY RANGE_N(BUSINESS_DT  BETWEEN DATE '2018-01-01' AND DATE '2021-12-31' EACH INTERVAL '1' DAY ,
	 UNKNOWN)
	ON COMMIT PRESERVE ROWS;
	
	insert into SUO_1
	select
		suo.posgr
		, suo.fblock_name
		, suo.EtalonPost
		, suo.urf_code
		, suo.SUBBRANCH_ID
		, suo.EVENT_TYPE_ID
		, suo.EVENT_DTTM
		, suo.saphr_id
		, suo.BUSINESS_DT
		, suo.TICKET_ID
		, suo.CURR_OPCAT_ID
		, suo.CURR_COUNTER
		, suo.ORDER_NUM
		, suo.TICKET_WAIT_TIME
		, suo.TICKET_SERV_TIME
		, suo.ticket_delay_flg
		, suo.urf_code_actual
		, suo.ticket_id_suffix
		, suo.ticket_tema
		, suo.time_suo_start
		, suo.time_suo_end
	from vt_SUO_1 as suo
	where suo.posgr in ('(�)��', '(�)�� ��', '(C)��', '(C)�� ��', '��', '�����', '����', '�����', '��')  ---- ����������� ���������� ��
	    and suo.urf_code is not null
	;

	collect stats column(saphr_id) on SUO_1;




/*****************************************************************************************************************/

-- ����: ������ ������ �� ������� ���.	

/*****************************************************************************************************************/



	------ ��������� �� SUO_0
	-- ������� ������������� ������ ��� �������
	set step = 4;
	CALL sbx_retail_ss.sau_DropTable('vt_suo_precalc');
	create multiset volatile table vt_suo_precalc as (
	select 
		suo.BUSINESS_DT
		, suo.SUBBRANCH_ID
		, suo.TICKET_ID
		, suo.ticket_id_suffix
		, suo.CURR_OPCAT_ID
		, suo.EVENT_TYPE_ID
		, suo.saphr_id
		, suo.EVENT_DTTM
		, suo.TICKET_WAIT_TIME
		, suo.CURR_COUNTER
		, suo.ticket_tema
		, suo.POSGR
		, dense_rank()
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix 
						order by suo.saphr_id) 							as prec_n_emploee
		, null												 			as n_emploee	-- ���-�� ���������� ���������� � �������
		
		, dense_rank()
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix 
						order by suo.EVENT_TYPE_ID) 					as prec_n_events
		, null															as n_events 	-- ���-�� �������������� ������� �� ������	
		
		, dense_rank()
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix 
						order by suo.CURR_OPCAT_ID) 					as prec_n_temas
		, null 															as n_temas		-- ���-�� ��� ������

		, min(case when suo.EVENT_TYPE_ID = 1 then suo.EVENT_DTTM else NULL end)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix)			as time_get_ticket	-- ����� ��������� ������	-- !!! ���������, ��� ������� ������ ������ ���������, ���� =2 ���������
		, first_value(suo.ticket_tema)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix
						order by suo.ORDER_NUM)							as fst_ticket_tema  -- ������ ���� � ������
		, min(case when suo.EVENT_TYPE_ID = 2 then suo.EVENT_DTTM else NULL end)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix)			as time_call_ticket	-- ������������������� ������
		
		, count(suo.EVENT_TYPE_ID) 
			 	over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
					, suo.TICKET_ID, suo.ticket_id_suffix) 				as cnt_event		-- ���������� �������� ������ (? ���������� ���� ���?)
		, dense_rank()
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix 
						order by suo.EVENT_TYPE_ID) 					as prec_cntd_event	-- 
		, max(suo.EVENT_TYPE_ID) 
			 	over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
					, suo.TICKET_ID, suo.ticket_id_suffix) 				as cntd_event 		-- ���������� ���������� �������� ������
		
		, null															as event_list 		-- ���������������������� �������� ������ � ����� ����
		, sum(case when suo.EVENT_TYPE_ID = 2 then 1 else 0 end)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix)			as n_calls 			-- ���-�� ������� ������� � ������
		, max(case when suo.EVENT_TYPE_ID = 2 then suo.TICKET_WAIT_TIME	else NULL end)	
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix)			as new_TICKET_WAIT_TIME		-- ����� �� ��������� ������ �� ������� ������
		, sum(suo.TICKET_WAIT_TIME) 
			 	over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
					, suo.TICKET_ID, suo.ticket_id_suffix) 				as sum_ticket_wait_time 	-- ����� ���� �������� � ������
		, max(suo.TICKET_WAIT_TIME)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
					, suo.TICKET_ID, suo.ticket_id_suffix)				as max_TICKET_WAIT_TIME		-- ������������ �������� ������
		
		, max(suo.EVENT_DTTM) 
			 	over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
						, suo.TICKET_ID, suo.ticket_id_suffix) 			as ticket_close_time		-- ��� ���� time_before_close
		
		, ((ticket_close_time - time_get_ticket)hour(4) to SECOND(6))   as time_before_close		-- ����� � ������� ������ ������ �� ��� �������� (max(event_dttm) � ������ -�time_get_ticket�� ������ SUO_0)
		, count(suo.CURR_COUNTER)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
					, suo.TICKET_ID, suo.ticket_id_suffix)  			as n_counter_change			--���������� ���� ����
		, sum(suo.ticket_delay_flg)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
					, suo.TICKET_ID, suo.ticket_id_suffix)				as sum_ticket_delay_flg 	-- ���-�� ������������ ������
		, max(suo.ticket_delay_flg)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
					, suo.TICKET_ID, suo.ticket_id_suffix)				as max_TICKET_DROP_FLG		-- ����� ������
		, max(suo.TICKET_SERV_TIME)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
					, suo.TICKET_ID, suo.ticket_id_suffix)				as max_TICKET_SERV_TIME		-- ������������ ����� ������������ ������� � ������ ��������� (� ����� ����)�
		, avg(suo.TICKET_SERV_TIME)
				over (partition by suo.BUSINESS_DT, suo.SUBBRANCH_ID
					, suo.TICKET_ID, suo.ticket_id_suffix)				as avg_TICKET_SERV_TIME		-- ������� ����� ������������ ������� � ������ ��������� (� ����� ����)�
		, suo.ORDER_NUM
	from vt_suo_0 suo
	where 0=0
		and suo.BUSINESS_DT = :rep_dt
	)with data
	primary index(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix)
	on commit preserve rows
	;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'precalc. (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_suo_precalc;


	-- ��������� �� ������ ������ 
	set step = 5;
	CALL sbx_retail_ss.sau_DropTable('vt_suo_precalc2');
	CREATE MULTISET VOLATILE TABLE vt_suo_precalc2 (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		n_emploee INTEGER,
		n_events INTEGER,
		n_temas INTEGER,
		cntd_event INTEGER,
		n_counter_change INTEGER)
	PRIMARY INDEX (BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix )
	ON COMMIT PRESERVE ROWS;
	
	insert into vt_suo_precalc2
	select 
		suo.BUSINESS_DT
		, suo.SUBBRANCH_ID
		, suo.TICKET_ID
		, suo.ticket_id_suffix
		, max(prec_n_emploee)			as n_emploee	-- ���-�� ���������� ���������� � �������
		, max(prec_n_events)			as n_events 	-- ���-�� �������������� ������� �� ������	
		, max(prec_n_temas) 			as n_temas		-- ���-�� ��� ������
		, max(prec_cntd_event) 			as cntd_event 	-- ���������� ���������� �������� ������
		, sum(case when EVENT_TYPE_ID in (3, 4, 5, 7) then 1 else 0 end) - 1	as n_counter_change			--���������� ���� ����
	from vt_suo_precalc suo
	where 0=0
	group by 
		suo.BUSINESS_DT
		, suo.SUBBRANCH_ID
		, suo.TICKET_ID
		, suo.ticket_id_suffix
	;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_suo_precalc2. (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_suo_precalc2;



	-- ����������� �� ������ ������. ����� ������. ������ (���������� )
	set step = 6;
	CALL sbx_retail_ss.sau_DropTable('vt_suo_precalc3');
	create multiset volatile table vt_suo_precalc3 as (
	select 
		distinct 
		suo.BUSINESS_DT
		, suo.SUBBRANCH_ID
		, suo.TICKET_ID
		, suo.ticket_id_suffix
		, ticket_close_time
		, time_before_close			as time_before_close		-- ����� � ������� ������ ������ �� ��� ��������
		, time_get_ticket			as time_get_ticket			-- ����� ��������� ������	-- !!! ���������, ��� ������� ������ ������ ���������, ���� =2 ���������
		, suo.fst_ticket_tema       as fst_ticket_tema          -- ������ ���� � ������
		, time_call_ticket			as time_call_ticket			-- ������������������� ������
		, cnt_event 				as cnt_event				-- ���������� �������� ������ (? ���������� ���� ���?)
		, n_calls					as n_calls 					-- ���-�� ������� ������� � ������
		, new_TICKET_WAIT_TIME		as new_TICKET_WAIT_TIME		-- ����� �� ��������� ������ �� ������� ������
		, sum_ticket_wait_time		as sum_ticket_wait_time 	-- ����� ���� �������� � ������
		, max_TICKET_WAIT_TIME		as max_TICKET_WAIT_TIME		-- ������������ �������� ������
		, sum_ticket_delay_flg		as sum_ticket_delay_flg 	-- ���-�� ������������ ������
		, max_TICKET_DROP_FLG		as max_TICKET_DROP_FLG		-- ����� ������
		, max_TICKET_SERV_TIME		as max_TICKET_SERV_TIME		-- ������������ ����� ������������ ������� � ������ ��������� (� ����� ����)�
		, avg_TICKET_SERV_TIME		as avg_TICKET_SERV_TIME		-- ������� ����� ������������ ������� � ������ ��������� (� ����� ����)�
	from vt_suo_precalc suo
	where 0=0
	)with data
	primary index(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix)
	on commit preserve rows
	;

	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_suo_precalc3;



	---- ������������ ������ ���������� ������� ������
	-- ������� ��� �������� ���������� �������. ��� ������������ ������
	set step = 7;
	CALL sbx_retail_ss.sau_DropTable('vt_dist_event');
	CREATE MULTISET VOLATILE TABLE vt_dist_event (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		EVENT_TYPE_ID BYTEINT,
		event_type VARCHAR(35) CHARACTER SET UNICODE NOT CASESPECIFIC,
		rn INTEGER,
		cnt_event INTEGER)
	PRIMARY INDEX ( BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix )
	ON COMMIT PRESERVE ROWS;
	
	insert into vt_dist_event
	-- ����� ���������� ����������� �������� � ������ ������� ���������� �� �������
	-- ����� ���� � ����������� �������� � ������ ��� ������������ ���������� ������
	select 
		dsuop.business_dt
		, dsuop.subbranch_id
		, dsuop.ticket_id
		, dsuop.ticket_id_suffix
		, dsuop.event_type_id
		, case dsuop.event_type_id 
			when 1 then '��������� ������'
			when 2 then '����� �������'
			when 3 then '��������� ������������'
			when 4 then '������� � ������ ����'
			when 5 then '������� � ������ ��������� ��������'
			when 6 then '����� ������'
			when 7 then '������������ ������'
			when 8 then '��������� ������� ����'
			when 9 then '��������� ������� ������������'
			when 10 then '��������� �������� �������'
			when 11 then '��������� ������ ���'
			else '-'
		end									as event_type
		, row_number() 
				over(partition by dsuop.business_dt, dsuop.subbranch_id, dsuop.ticket_id, dsuop.ticket_id_suffix
					order by dsuop.pre_rn)     as rn	-- �������������� ���������� �������� ������� �� �������
		, count(*) 
				over(partition by dsuop.business_dt, dsuop.subbranch_id, dsuop.ticket_id, dsuop.ticket_id_suffix) as cnt_event
	from (-- ���������������� �����
			-- rn (���� ������� ������ ������) ��� �������������� �������� ������ ������ �� �������. 
			--   ���� ����� ���������, �� ������ ���� ������ � ������� �� �������
			-- event_rn(����� ������� ������ ������ ����� �������� ���� �� ����)
		select 
			suop.business_dt
			, suop.subbranch_id
			, suop.ticket_id
			, suop.ticket_id_suffix
			, event_dttm
			, suop.event_type_id
			, row_number() 
				over(partition by suop.business_dt, suop.subbranch_id, suop.ticket_id, suop.ticket_id_suffix
					order by suop.order_num)     	as pre_rn	-- �������������� �������� ������� �� �������
			, row_number() 
				over(partition by suop.business_dt, suop.subbranch_id
						, suop.ticket_id, suop.ticket_id_suffix, suop.event_type_id
					order by suop.order_num)     	as event_rn	-- �������������� ������ ��������� ������� ������ �� �������
		from vt_suo_precalc suop
		qualify event_rn = 1  -- ���� ������ ������ ��������� ������� � ������
		)dsuop
	;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_dist_event. (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_dist_event;


	-- ����� ���������� ������ ������� ������
	set step = 8;
	CALL sbx_retail_ss.sau_DropTable('vt_event_list');
	create multiset volatile table vt_event_list (
	business_dt date, 
	subbranch_id integer, 
	ticket_id char(4), 
	ticket_id_suffix integer,
	event_list varchar(3000)
	)
	primary index(business_dt, subbranch_id, ticket_id, ticket_id_suffix)
	on commit preserve rows;

	INSERT INTO vt_event_list
	WITH RECURSIVE base (c_business_dt, c_subbranch_id, c_ticket_id, c_ticket_id_suffix
						, event_type_id, event_type, rn, cnt_event, event_list)
	AS
	(
	SELECT 
	    business_dt     	AS c_business_dt
	    , subbranch_id 		AS c_subbranch_id
	    , ticket_id 		AS c_ticket_id 
		, ticket_id_suffix 	AS c_ticket_id_suffix
		, event_type_id
		, event_type
	    , rn
		, cnt_event
	    , TRIM(CAST(event_type AS VARCHAR(3000))) AS event_list
	FROM vt_dist_event
	WHERE rn = 1
	UNION ALL
	SELECT 
		c.business_dt     		AS c_business_dt
	    , c.subbranch_id 		AS c_subbranch_id
	    , c.ticket_id 			AS c_ticket_id 
		, c.ticket_id_suffix 	AS c_ticket_id_suffix
		, c.event_type_id
		, c.event_type
	    , c.rn
		, c.cnt_event
	    , b.event_list !! ', ' !! TRIM(CAST (c.event_type AS VARCHAR(3000))) 	AS event_list
	FROM vt_dist_event c
	JOIN base b
	    ON b.c_business_dt = c.business_dt
	    AND b.c_subbranch_id = c.subbranch_id
		AND b.c_ticket_id = c.ticket_id
		AND b.c_ticket_id_suffix = c.ticket_id_suffix
	    AND b.rn + 1 = c.rn
	)
	SELECT 
		c_business_dt     
	    , c_subbranch_id 	
	    , c_ticket_id 	
		, c_ticket_id_suffix
		, event_list
	FROM base
	WHERE 
		rn=cnt_event;


	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_event_list (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));

	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_event_list;

	
	
	-- ������� ��� �������� ���������� �����. ��� ������������ ������ ����� �� ��� (role_list_suo)
	CALL sbx_retail_ss.sau_DropTable('vt_dist_suo_posgr');
	CREATE MULTISET VOLATILE TABLE vt_dist_suo_posgr (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		posgr VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		rn INTEGER,
		cnt_posgr INTEGER)
	PRIMARY INDEX ( BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix )
	ON COMMIT PRESERVE ROWS;

	insert into vt_dist_suo_posgr
	-- ����� ���������� ����������� �������� � ������ ������� ���������� �� �������
	-- ����� ���� � ����������� �������� � ������ ��� ������������ ���������� ������
	select 
		dsuop.business_dt
		, dsuop.subbranch_id
		, dsuop.ticket_id
		, dsuop.ticket_id_suffix
		, dsuop.posgr
		, row_number() 
				over(partition by dsuop.business_dt, dsuop.subbranch_id, dsuop.ticket_id, dsuop.ticket_id_suffix
					order by dsuop.pre_rn)     as rn	-- �������������� ���������� �������� ������� �� �������
		, count(*) 
				over(partition by dsuop.business_dt, dsuop.subbranch_id, dsuop.ticket_id, dsuop.ticket_id_suffix) as cnt_posgr
	from (-- ���������������� �����
			-- pre_rn (���� ���� ������ ������) ��� �������������� ����� ������ ������ �� �������. 
			-- posgr_rn(����� ������� ������ ������ ����� �������� ���� �� ����)
		select 
			suop.business_dt
			, suop.subbranch_id
			, suop.ticket_id
			, suop.ticket_id_suffix
			, event_dttm
			, suop.posgr
			, row_number() 
				over(partition by suop.business_dt, suop.subbranch_id, suop.ticket_id, suop.ticket_id_suffix
					order by suop.event_dttm, suop.order_num)     		as pre_rn	-- �������������� �������� ������� �� �������
			, row_number() 
				over(partition by suop.business_dt, suop.subbranch_id
						, suop.ticket_id, suop.ticket_id_suffix, suop.posgr
					order by suop.event_dttm, suop.order_num)     		as posgr_rn	-- ����� ��������� ���� � ������
		from vt_suo_precalc suop
		where 0=0
			and suop.posgr is not null
		qualify posgr_rn = 1  -- ���� ������ ������ ��������� ������� � ������
		)dsuop
	;

	-- ���������� ������ ����� �� ���
	CALL sbx_retail_ss.sau_DropTable('vt_suo_role_list');
	create multiset volatile table vt_suo_role_list (
	business_dt date, 
	subbranch_id integer, 
	ticket_id char(4), 
	ticket_id_suffix integer,
	posgr_list varchar(3000)
	)
	primary index(business_dt, subbranch_id, ticket_id, ticket_id_suffix)
	on commit preserve rows;

	INSERT INTO vt_suo_role_list
	WITH RECURSIVE base (c_business_dt, c_subbranch_id, c_ticket_id, c_ticket_id_suffix
						, rn, cnt_posgr, posgr_list)
	AS
	(
	SELECT 
	    business_dt     	AS c_business_dt
	    , subbranch_id 		AS c_subbranch_id
	    , ticket_id 		AS c_ticket_id 
		, ticket_id_suffix 	AS c_ticket_id_suffix
	    , rn
		, cnt_posgr
	    , TRIM(CAST(posgr AS VARCHAR(3000))) AS posgr_list
	FROM vt_dist_suo_posgr
	WHERE rn = 1
	UNION ALL
	SELECT 
		c.business_dt     		AS c_business_dt
	    , c.subbranch_id 		AS c_subbranch_id
	    , c.ticket_id 			AS c_ticket_id 
		, c.ticket_id_suffix 	AS c_ticket_id_suffix
	    , c.rn
		, c.cnt_posgr
	    , b.posgr_list !! ', ' !! TRIM(CAST (c.posgr AS VARCHAR(3000))) 	AS posgr_list
	FROM vt_dist_suo_posgr c
	JOIN base b
	    ON b.c_business_dt = c.business_dt
	    AND b.c_subbranch_id = c.subbranch_id
		AND b.c_ticket_id = c.ticket_id
		AND b.c_ticket_id_suffix = c.ticket_id_suffix
	    AND b.rn + 1 = c.rn
	)
	SELECT 
		c_business_dt     
	    , c_subbranch_id 	
	    , c_ticket_id 	
		, c_ticket_id_suffix
		, posgr_list
	FROM base
	WHERE 
		rn = cnt_posgr;
	
	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_suo_role_list. (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	

	-- ������� �������� ���
	set step = 9;
	CALL sbx_retail_ss.sau_DropTable('vt_suo_agg');
	
	create multiset volatile table vt_suo_agg as (
	select 
		pc3.BUSINESS_DT
		, pc3.SUBBRANCH_ID
		, pc3.TICKET_ID
		, pc3.ticket_id_suffix
		, pc2.n_emploee					-- ���-�� ���������� ���������� � �������
		, pc2.n_events 					-- ���-�� �������������� ������� �� ������	
		, pc2.n_temas					-- ���-�� ��� ������
		, pc3.time_get_ticket			-- ����� ��������� ������	
		, pc3.fst_ticket_tema      		-- ������ ���� ������
		, pc3.time_call_ticket			-- ������������������� ������
		, pc3.cnt_event					-- ���������� �������� ������ (? ���������� ���� ���?)
		, pc2.cntd_event 				-- ���������� ���������� �������� ������
		, evlist.event_list 			-- ���������������������� �������� ������ � ����� ����
		, pc3.n_calls 					-- ���-�� ������� ������� � ������
		, pc3.new_TICKET_WAIT_TIME		-- ����� �� ��������� ������ �� ������� ������
		, pc3.sum_ticket_wait_time 		-- ����� ���� �������� � ������
		, pc3.max_TICKET_WAIT_TIME		-- ������������ �������� ������
		, pc3.time_before_close			-- ����� � ������� ������ ������ �� ��� ��������
		, pc2.n_counter_change			-- ���������� ���� ����
		, pc3.sum_ticket_delay_flg 		-- ���-�� ������������ ������
		, pc3.max_TICKET_DROP_FLG		-- ����� ������
		, pc3.max_TICKET_SERV_TIME		-- ������������ ����� ������������ ������� � ������ ��������� (� ����� ����)�
		, pc3.avg_TICKET_SERV_TIME		-- ������� ����� ������������ ������� � ������ ��������� (� ����� ����)�
		, relel.posgr_list			as role_list_suo	-- ���� ���� ����������� (� ����� ����) (�� ���)
	from vt_suo_precalc3 pc3
	left join vt_suo_precalc2 pc2
		on pc3.BUSINESS_DT = pc2.BUSINESS_DT
		and pc3.SUBBRANCH_ID = pc2.SUBBRANCH_ID
		and pc3.TICKET_ID = pc2.TICKET_ID
		and pc3.ticket_id_suffix = pc2.ticket_id_suffix
	left join vt_event_list evlist
		on pc3.BUSINESS_DT = evlist.BUSINESS_DT
		and pc3.SUBBRANCH_ID = evlist.SUBBRANCH_ID
		and pc3.TICKET_ID = evlist.TICKET_ID
		and pc3.ticket_id_suffix = evlist.ticket_id_suffix
	left join vt_suo_role_list relel
		on pc3.BUSINESS_DT = relel.BUSINESS_DT
		and pc3.SUBBRANCH_ID = relel.SUBBRANCH_ID
		and pc3.TICKET_ID = relel.TICKET_ID
		and pc3.ticket_id_suffix = relel.ticket_id_suffix
	where 0=0
	)with data
	primary index(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix)
	on commit preserve rows
	;
	
	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_suo_agg. (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	
	
/*****************************************************************************************************************/

-- ����: �������� ���� 

/*****************************************************************************************************************/

	-- 2.1)
	/*
	����� ������ �� ���� "001_MIS_Retail_Reporting".v_AS_FS_oper_non_agg �� �������� ������ (OPER_DATE).
	������� ������ (user_login)
	USER_LOGIN1
	�������� ����� ����� @ ( trim(upper(strtok(a.user_login,'@',1))) as USER_LOGIN1).
	������� ����������� ��������: ����� ���������, ������������� ������� � �.�. (function_id not in
	(55,134,155,157,161,175,183,225,262,263,266,271,272,277))
	��������� ��������� ������ (saphr_id) �� ("001_mis_retail_channel".vsp_asfsb_dic_logins)
	��������� �� ���������������� ����� tb � TERBANK � �� ������ USER_LOGIN1 � (employee_login).
	��������� ����������� �������� ���� �� "001_mis_retail_channel".vsp_buf_audit_func_dic
	��������� �� ��������������� FUNCTION_ID � ID �� �����������
	������� (name) � FUNCTION_ID
	�������� asfs
	*/

	---- �� �� + ����������
	set step = 10;
	CALL sbx_retail_ss.sau_DropTable('ASFS_tab');
	CREATE MULTISET VOLATILE TABLE ASFS_tab (
		ID BIGINT,
		DATETIME TIMESTAMP(0),
		OPER_DATE DATE FORMAT 'YY/MM/DD',
		TERBANK INTEGER,
		OSB_NUMBER INTEGER,
		VSP_NUMBER INTEGER,
		user_login VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
		FUNCTION_ID INTEGER,
		f_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		SUMM_RUB FLOAT,
		COMMIS_SUMM_RUB FLOAT,
		mega_id INTEGER,
		CID VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
		CID_type INTEGER,
		USER_LOGIN1 VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
		saphr_id BIGINT,
		next_oper TIME(6),
		date_rep DATE FORMAT 'YY/MM/DD',
		ter INTEGER)
	PRIMARY INDEX ( saphr_id )
	ON COMMIT PRESERVE ROWS;
	
	insert into ASFS_tab
	select
	    asfs.ID
	    , asfs.DATETIME
		, asfs.OPER_DATE
	    , asfs.TERBANK
	    , asfs.OSB_NUMBER
	    , asfs.VSP_NUMBER
	    , asfs.user_login
	    , asfs.FUNCTION_ID
	    , d.name 				as f_name
	    , asfs.SUMM_RUB
	    , asfs.COMMIS_SUMM_RUB
	    , asfs.mega_id
	    , asfs.CID
	    , asfs.CID_type
	    , trim(upper(strtok(asfs.user_login,'@',1))) 	as USER_LOGIN1
	    , ul.saphr_id
	    , asfs.datetime(time) as next_oper
	    , asfs.datetime(date) as date_rep 
	    , asfs.TERBANK(int) as ter
	from "001_MIS_Retail_Reporting".v_AS_FS_oper_non_agg asfs
	join "001_mis_retail_channel".vsp_asfsb_dic_logins ul
	    on asfs.TERBANK = ul.tb
	    and USER_LOGIN1 = Upper(ul.employee_login)
	left join "001_mis_retail_channel".vsp_buf_audit_func_dic as d 
		on asfs.FUNCTION_ID=d.id
	where 0=0
		and asfs.OPER_DATE = :rep_dt
		-- ��������� ���������� �������� ����, ������������� � ����� ��������� ������������� ������� 175,272 
		and asfs.function_id not in (55,134,155,157,161,175,183,225,262,263,266,271,272,277)			
	;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'ASFS_tab (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));

	collect stats ASFS_tab column(saphr_id);
	 
	 

	-- 2.2)
	-- ������������� ���� (��������� client_dk)
	set step = 11;
	CALL sbx_retail_ss.sau_DropTable('ASFS_1');
	CREATE MULTISET VOLATILE TABLE ASFS_1 (
		ID BIGINT,
		DATETIME TIMESTAMP(0),
		OPER_DATE DATE FORMAT 'YY/MM/DD',
		TERBANK INTEGER,
		OSB_NUMBER INTEGER,
		VSP_NUMBER INTEGER,
		user_login VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
		FUNCTION_ID INTEGER,
		f_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		SUMM_RUB FLOAT,
		COMMIS_SUMM_RUB FLOAT,
		mega_id INTEGER,
		CID VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
		CID_type INTEGER,
		USER_LOGIN1 VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
		saphr_id BIGINT,
		next_oper TIME(6),
		date_rep DATE FORMAT 'YY/MM/DD',
		ter INTEGER,
		client_dk BIGINT)
	PRIMARY INDEX ( saphr_id )
	PARTITION BY RANGE_N(date_rep  BETWEEN DATE '2018-01-01' AND DATE '2021-12-31' EACH INTERVAL '1' DAY ,
	UNKNOWN)
	ON COMMIT PRESERVE ROWS;
	
	insert into ASFS_1
	select	
	    asfs.ID
	    , asfs.DATETIME
		, asfs.OPER_DATE
	    , asfs.TERBANK
	    , asfs.OSB_NUMBER
	    , asfs.VSP_NUMBER
	    , asfs.user_login
	    , asfs.FUNCTION_ID
	    , asfs.f_name
	    , asfs.SUMM_RUB
	    , asfs.COMMIS_SUMM_RUB
	    , asfs.mega_id
	    , cast(substr(asfs.CID, 1, 40) as varchar(40)) as CID
	    , asfs.CID_type
	    , USER_LOGIN1
	    , saphr_id
	    , asfs.next_oper
	    , asfs.date_rep 
	    , asfs.ter
		, TIDS.client_dk		
	from ASFS_tab as asfs 
	---��������� �������� ��� ��������� �������
	left join "001_MIS_RETAIL_CHANNEL".VSP_WORKTIME_info_system_id_mega_cod@PROMTD as SYSTEMS
		on SYSTEMS.id_mega = asfs.ter
	left join sbx_retail_data.lnk_mdm_clnt_host_id_h as TIDS ----��� ����� ������������ �������� ���� ������� � ���������� ��������
		on cast(TIDS.client_host_id as varchar(70)) = asfs.cid 
		AND SYSTEMS.info_system_id = TIDS.info_system_id 
		AND asfs.OPER_DATE between TIDS.row_actual_from_dt and TIDS.row_actual_to_dt
	;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'ASFS_1 (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));

	collect stats column(saphr_id) on ASFS_1;



/*****************************************************************************************************************/

-- ����: ���������� ���� � ��� 

/*****************************************************************************************************************/


	-- 2.3)

	--- ��������� ������ � ��������
	
	
	set step = 12;
	delete from SBX_RETAIL_SS.vsp_asfs_suo_oper where business_dt = :rep_dt;	--date'2020-03-04';	-- !!!
	insert into SBX_RETAIL_SS.vsp_asfs_suo_oper
	select 
		suo.BUSINESS_DT,
		suo.SUBBRANCH_ID,
		suo.TICKET_ID,
		suo.ticket_id_suffix,
		suo.ticket_tema,
		suo.POSGR,
		suo.fblock_name,
		suo.EtalonPost,
		suo.urf_code,
		suo.EVENT_TYPE_ID,
		suo.EVENT_DTTM,
		suo.saphr_id,
		suo.CURR_OPCAT_ID,
		suo.CURR_COUNTER,
		NULL 	as USER_ID,				--suo.USER_ID,
		NULL 	as CUSTOM_PARAM_1,		-- suo.CUSTOM_PARAM_1,
		NULL 	as CUSTOM_PARAM_2,		-- suo.CUSTOM_PARAM_2,
		NULL 	as CUSTOM_PARAM_3,		-- suo.CUSTOM_PARAM_3,
		NULL 	as RECEIVE_DTTM,		-- suo.RECEIVE_DTTM,
		NULL 	as LOAD_ID,				-- suo.LOAD_ID,
		suo.ORDER_NUM,
		suo.TICKET_WAIT_TIME,
		suo.TICKET_SERV_TIME,
		NULL 	as TICKET_REG_FLG,			-- suo.TICKET_REG_FLG,
		NULL 	as ticket_transfer_cat_flg,		-- suo.ticket_transfer_cat_flg,
		NULL 	as TICKET_SERV_FLG,			-- suo.TICKET_SERV_FLG,
		NULL 	as ticket_serv_flg_kpi,		-- suo.ticket_serv_flg_kpi,
		NULL 	as ticket_transfer_o_flg,		-- suo.ticket_transfer_o_flg,
		NULL 	as ticket_transfer_c_flg,		-- suo.ticket_transfer_c_flg,
		suo.ticket_delay_flg,
		NULL 	as TICKET_DROP_FLG,			-- suo.TICKET_DROP_FLG,
		NULL 	as bad_user_by_ticket_cnt,		-- suo.bad_user_by_ticket_cnt,
		NULL 	as bad_user_by_ticket_flg,		-- suo.bad_user_by_ticket_flg,
		NULL 	as TICKET_SERV_FLG_TECH,		-- suo.TICKET_SERV_FLG_TECH,
		suo.urf_code_actual,
		NULL 	as is_short,				-- suo.is_short,
		NULL 	as TICKET_SERV_FLG_time,	-- suo.TICKET_SERV_FLG_time,
		NULL 	as role_id,					-- suo.role_id,
		NULL 	as ticket_2nd_reg_flg,		-- suo.ticket_2nd_reg_flg,
		NULL 	as rn_stat,					-- suo.rn_stat,
		NULL 	as end_ticket,				-- suo.end_ticket,
		suo.time_suo_start,
		suo.time_suo_end
	    , asfs.ID
	    , asfs.DATETIME
		, asfs.OPER_DATE
	    , asfs.TERBANK
	    , asfs.OSB_NUMBER
	    , asfs.VSP_NUMBER
	    , asfs.user_login
	    , asfs.FUNCTION_ID
	    , asfs.f_name
	    , asfs.SUMM_RUB
	    , asfs.COMMIS_SUMM_RUB
	    , asfs.mega_id
	    , asfs.CID
	    , asfs.CID_type
	    , asfs.USER_LOGIN1
	    , asfs.next_oper
	    , asfs.date_rep 
	    , asfs.ter
		, asfs.client_dk
	from SUO_1 as suo
	join ASFS_1 as asfs	
	  	on asfs.saphr_id = suo.saphr_id
		and asfs.date_rep = suo.business_dt
		and asfs.next_oper between suo.time_suo_start and suo.time_suo_end
		and asfs.client_dk is not null
	;
	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'SBX_RETAIL_SS.vsp_asfs_suo_oper (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));

	collect stats column(BUSINESS_DT, SUBBRANCH_ID, ticket_id, ticket_id_suffix) on SBX_RETAIL_SS.vsp_asfs_suo_oper;



	------- �������� ������ � ����� ��� 1 ������ ��� ����������
	set step = 14;
	CALL sbx_retail_ss.sau_DropTable('vt_DUBL');
	
	CREATE MULTISET VOLATILE TABLE vt_DUBL (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		ctn INTEGER)
	PRIMARY INDEX (BUSINESS_DT, SUBBRANCH_ID, ticket_id, ticket_id_suffix)
	ON COMMIT PRESERVE ROWS;
	
	insert into vt_DUBL
	Select a.BUSINESS_DT, a.SUBBRANCH_ID, a.ticket_id, a.ticket_id_suffix, count(distinct client_dk) ctn
	from SBX_RETAIL_SS.vsp_asfs_suo_oper as a 	
	where a.BUSINESS_DT = :rep_dt	
		and client_dk is not null
	group by a.BUSINESS_DT, a.SUBBRANCH_ID, a.ticket_id, a.ticket_id_suffix
	having count(distinct client_dk) > 1
	;

	collect stats column(BUSINESS_DT, SUBBRANCH_ID, ticket_id, ticket_id_suffix) on vt_DUBL;

	-- ������� ������-�������� � ������� �� �������
	delete t
	from SBX_RETAIL_SS.vsp_asfs_suo_oper as t, vt_DUBL dubl
	where t.business_dt = dubl.business_dt
		and t.SUBBRANCH_ID = dubl.SUBBRANCH_ID
		and t.ticket_id = dubl.ticket_id
		and t.ticket_id_suffix = dubl.ticket_id_suffix;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_DUBL (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));





/*****************************************************************************************************************/

-- ����: ��������� �������� �� ���� ��  

/*****************************************************************************************************************/

	------ 	2.4)   ���������� ������� �� ����� � ������� �������� ����������. ������ (�)�� ('(�)��', '(�)�� ��', '(C)��', '(C)�� ��')
	-- �������� ������ ��� ���� �������
	set step = 13;
	CALL sbx_retail_ss.sau_DropTable('vt_suo_asfs_role_MO');
	CREATE MULTISET VOLATILE TABLE vt_suo_asfs_role_MO (
		ticket_tema VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		POSGR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		fblock_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		EtalonPost VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		urf_code VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		SUBBRANCH_ID BIGINT,
		EVENT_TYPE_ID BYTEINT,
		EVENT_DTTM TIMESTAMP(6),
		saphr_id INTEGER,
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		CURR_OPCAT_ID SMALLINT,
		CURR_COUNTER SMALLINT,
		ORDER_NUM INTEGER,
		TICKET_WAIT_TIME INTEGER,
		TICKET_SERV_TIME INTEGER,
		ticket_delay_flg BYTEINT,
		urf_code_actual VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		time_suo_start TIME(6),
		time_suo_end TIME(6),
		ID BIGINT,
		DATETIME TIMESTAMP(0),
		OPER_DATE DATE FORMAT 'YY/MM/DD',
		TERBANK INTEGER,
		OSB_NUMBER INTEGER,
		VSP_NUMBER INTEGER,
		user_login VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
		FUNCTION_ID INTEGER,
		f_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		SUMM_RUB FLOAT,
		COMMIS_SUMM_RUB FLOAT,
		mega_id INTEGER,
		CID VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
		CID_type INTEGER,
		USER_LOGIN1 VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
		next_oper TIME(6),
		date_rep DATE FORMAT 'YY/MM/DD',
		ter INTEGER,
		client_dk BIGINT)
	PRIMARY INDEX (BUSINESS_DT, SUBBRANCH_ID, ticket_id, ticket_id_suffix)
	ON COMMIT PRESERVE ROWS;
	
	insert into vt_suo_asfs_role_MO
	select 
		a.ticket_tema ,
		a.POSGR ,
		a.fblock_name ,
		a.EtalonPost ,
		a.urf_code ,
		a.SUBBRANCH_ID ,
		a.EVENT_TYPE_ID ,
		a.EVENT_DTTM ,
		a.saphr_id ,
		a.BUSINESS_DT ,
		a.TICKET_ID ,
		a.CURR_OPCAT_ID ,
		a.CURR_COUNTER ,
		a.ORDER_NUM ,
		a.TICKET_WAIT_TIME ,
		a.TICKET_SERV_TIME ,
		a.ticket_delay_flg ,
		a.urf_code_actual ,
		a.ticket_id_suffix ,
		a.time_suo_start ,
		a.time_suo_end ,
		a.ID ,
		a.DATETIME ,
		a.OPER_DATE ,
		a.TERBANK ,
		a.OSB_NUMBER ,
		a.VSP_NUMBER ,
		a.user_login ,
		a.FUNCTION_ID ,
		a.f_name ,
		a.SUMM_RUB ,
		a.COMMIS_SUMM_RUB ,
		a.mega_id ,
		a.CID ,
		a.CID_type ,
		a.USER_LOGIN1,
		a.next_oper,
		a.date_rep ,
		a.ter ,
		a.client_dk 
	from SBX_RETAIL_SS.vsp_asfs_suo_oper as a	
	where 
		a.posgr in ('(�)��', '(�)�� ��', '(C)��', '(C)�� ��')
	;
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, ticket_id, ticket_id_suffix) on vt_suo_asfs_role_MO;




	set step = 15;	-- ������ ���-���� �� ���� ��. ����� ��������� ������ ����.
	delete from SBX_RETAIL_SS.vsp_asfs_suo_ALL_ROLES where business_dt = :rep_dt;	
	insert into SBX_RETAIL_SS.vsp_asfs_suo_ALL_ROLES
	Select 
		a2.ticket_tema
		, a2.POSGR
		, a2.fblock_name
		, a2.EtalonPost
		, a2.urf_code
		, a2.SUBBRANCH_ID
		, a2.EVENT_TYPE_ID
		, a2.EVENT_DTTM
		, a2.saphr_id
		, a2.BUSINESS_DT
		, a2.TICKET_ID
		, a2.CURR_OPCAT_ID
		, a2.CURR_COUNTER
		, NULL     as USER_ID			-- , a2.USER_ID
		, NULL     as CUSTOM_PARAM_1			-- a2.CUSTOM_PARAM_1		-- 
		, NULL     as CUSTOM_PARAM_2		-- a2.CUSTOM_PARAM_2		-- 
		, NULL     as CUSTOM_PARAM_3		-- a2.CUSTOM_PARAM_3		-- 
		, NULL     as RECEIVE_DTTM		-- a2.RECEIVE_DTTM		-- 
		, NULL     as LOAD_ID		-- a2.LOAD_ID			-- 
		, a2.ORDER_NUM
		, a2.TICKET_WAIT_TIME
		, a2.TICKET_SERV_TIME
		, NULL     as TICKET_REG_FLG		-- a2.TICKET_REG_FLG		-- 
		, NULL     as ticket_transfer_cat_flg		-- a2.ticket_transfer_cat_flg		-- 
		, NULL     as TICKET_SERV_FLG		-- a2.TICKET_SERV_FLG		-- 
		, NULL     as ticket_serv_flg_kpi		-- a2.ticket_serv_flg_kpi		-- 
		, NULL     as ticket_transfer_o_flg		-- a2.ticket_transfer_o_flg		-- 
		, NULL     as ticket_transfer_c_flg		-- a2.ticket_transfer_c_flg		-- 
		, a2.ticket_delay_flg
		, NULL     as TICKET_DROP_FLG		-- a2.TICKET_DROP_FLG		-- 
		, NULL     as bad_user_by_ticket_cnt		-- a2.bad_user_by_ticket_cnt		-- 
		, NULL     as bad_user_by_ticket_flg		-- a2.bad_user_by_ticket_flg		-- 
		, NULL     as TICKET_SERV_FLG_TECH		-- a2.TICKET_SERV_FLG_TECH		-- 
		, a2.urf_code_actual
		, NULL     as is_short		-- a2.is_short		-- 
		, NULL     as TICKET_SERV_FLG_time		-- a2.TICKET_SERV_FLG_time		-- 
		, NULL     as role_id		-- a2.role_id		-- 
		, NULL     as ticket_2nd_reg_flg		-- a2.ticket_2nd_reg_flg		-- 
		, a2.ticket_id_suffix
		, NULL     as rn_stat		-- a2.rn_stat		-- 
		, NULL     as end_ticket		-- a2.end_ticket		-- 
		, a2.time_suo_start
		, a2.time_suo_end
		, a2.ID
		, a2.DATETIME
		, a2.OPER_DATE
		, a2.TERBANK
		, a2.OSB_NUMBER
		, a2.VSP_NUMBER
		, a2.user_login
		, a2.FUNCTION_ID
		, a2.f_name
		, a2.SUMM_RUB
		, a2.COMMIS_SUMM_RUB
		, a2.mega_id
		, a2.CID
		, a2.CID_type
		, a2.USER_LOGIN1
		, a2.next_oper
		, a2.date_rep
		, a2.ter
		, a2.client_dk
		, row_number()over(partition by a2.BUSINESS_DT, a2.SUBBRANCH_ID, a2.ticket_id, a2.ticket_id_suffix order by a2.event_dttm, next_oper) 				as rn_oper -- ����� ��������
		, row_number()over(partition by a2.BUSINESS_DT, a2.SUBBRANCH_ID, a2.ticket_id, a2.ticket_id_suffix order by a2.event_dttm desc, next_oper desc) 	as rn_oper_desc -- ����� ��������
		, first_value(a2.f_name)over(partition by a2.BUSINESS_DT, a2.SUBBRANCH_ID, a2.ticket_id, a2.ticket_id_suffix order by a2.event_dttm, next_oper ) 			as first_oper 	-- ������ ���������
		, first_value(a2.f_name)over(partition by a2.BUSINESS_DT, a2.SUBBRANCH_ID, a2.ticket_id, a2.ticket_id_suffix order by a2.event_dttm desc, next_oper desc) 	as last_oper 	-- ��������� ���������
	from vt_suo_asfs_role_MO as a2	
	;
	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'SBX_RETAIL_SS.vsp_asfs_suo_ALL_ROLES (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));

	collect stats column(BUSINESS_DT, SUBBRANCH_ID, ticket_id, ticket_id_suffix) on SBX_RETAIL_SS.vsp_asfs_suo_ALL_ROLES;


/*****************************************************************************************************************/

-- ����: ���������� ������ �����

/*****************************************************************************************************************/

	-- ���������� � ������� sau_vsp_tmp_asfs_suo_ticket_tab1 ��������� �����
	set step = 16;   
	call SBX_RETAIL_SS.sp_vsp_asfs_add_role_tickets(:rep_dt, '��');
	/*call SBX_RETAIL_SS.sp_vsp_asfs_add_role_tickets(:rep_dt, '�����');
	call SBX_RETAIL_SS.sp_vsp_asfs_add_role_tickets(:rep_dt, '����');
	call SBX_RETAIL_SS.sp_vsp_asfs_add_role_tickets(:rep_dt, '�����');
	call SBX_RETAIL_SS.sp_vsp_asfs_add_role_tickets(:rep_dt, '��');*/
	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, '(' || v_char_dt || ') ���������� ����� � SBX_RETAIL_SS.vsp_asfs_suo_ALL_ROLES', ACTIVITY_COUNT, current_timestamp(0));













/*****************************************************************************************************************/

-- ����: ������ ������ ����

/*****************************************************************************************************************/



	----asfs agregation
	-- ��������� ������������� ������� (������ ��������, ��������� ��������)
	set step = 17;
	CALL sbx_retail_ss.sau_DropTable('vt_asfs_suo_precalc');
	
	CREATE MULTISET VOLATILE TABLE vt_asfs_suo_precalc (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		client_dk BIGINT,
		FUNCTION_ID INTEGER,
		saphr_id INTEGER,
		EVENT_TYPE_ID BYTEINT,
		CURR_OPCAT_ID SMALLINT,
		next_oper TIME(6),
		EVENT_DTTM TIMESTAMP(6),
		POSGR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		f_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ID BIGINT,
		oper_code VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		oper_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		first_oper VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		last_oper VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		pre_rn INTEGER,
		POSGR_rn INTEGER,
		func_rn INTEGER,
		saphr_id_rn INTEGER,
		func_tick_rn INTEGER,
		func_tick_rn_desc INTEGER,
		prec_summ_oper INTEGER)
	PRIMARY INDEX ( BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix )
	ON COMMIT PRESERVE ROWS;
	
	insert into vt_asfs_suo_precalc
	select 
		asfs.BUSINESS_DT
		, asfs.SUBBRANCH_ID
		, asfs.ticket_id
		, asfs.ticket_id_suffix			-- ������� ������(���� ������� ������������)
		, asfs.client_dk
		, asfs.function_id
		, asfs.saphr_id
		, asfs.event_type_id
		, asfs.CURR_OPCAT_ID
		, asfs.next_oper
		, asfs.event_dttm
		, asfs.POSGR
		, asfs.f_name
		, asfs.ID
		, dic_func.code 				as oper_code
		, dic_func.name 				as oper_name
		, first_value(dic_func.name)over (partition by asfs.BUSINESS_DT, asfs.SUBBRANCH_ID
					, asfs.TICKET_ID, asfs.ticket_id_suffix
					order by asfs.next_oper, asfs.ID 
			)							as first_oper
		, first_value(dic_func.name)over (partition by asfs.BUSINESS_DT, asfs.SUBBRANCH_ID
					, asfs.TICKET_ID, asfs.ticket_id_suffix
					order by asfs.next_oper desc, asfs.ID desc
			)							as last_oper
		, row_number() 
				over(partition by asfs.business_dt, asfs.subbranch_id, asfs.ticket_id, asfs.ticket_id_suffix
					order by asfs.event_dttm, asfs.event_type_id)     	as pre_rn	-- �������������� �������� �� �������
		-- ��� ������ �����
		, row_number() 
			over(partition by asfs.business_dt, asfs.subbranch_id
					, asfs.ticket_id, asfs.ticket_id_suffix, asfs.POSGR
				order by asfs.event_dttm, asfs.event_type_id)     		as POSGR_rn	-- �������������� ������ ��������� ���� �� �������
		-- ��� ������ ��������
		, row_number() 
			over(partition by asfs.business_dt, asfs.subbranch_id
					, asfs.ticket_id, asfs.ticket_id_suffix, asfs.f_name
				order by asfs.next_oper, asfs.ID)     					as func_rn	-- �������������� ������ ��������� �������� �� �������
		
		-- ��� ��������� ���������� 
		/*, row_number() 
			over(partition by asfs.business_dt, asfs.subbranch_id
					, asfs.ticket_id, asfs.ticket_id_suffix, asfs.saphr_id
				order by asfs.event_dttm, asfs.event_type_id)     		as saphr_id_rn*/	-- �������������� ������ ��������� ��������� �� �������
			
		, row_number() 
		over(partition by asfs.business_dt, asfs.subbranch_id
				, asfs.ticket_id, asfs.ticket_id_suffix, asfs.posgr
			order by asfs.event_dttm, asfs.event_type_id)     			as saphr_id_rn	-- �������������� ������ ��������� ��������� �� �������
			
		, row_number() 
			over(partition by asfs.business_dt, asfs.subbranch_id
					, asfs.ticket_id, asfs.ticket_id_suffix
				order by asfs.next_oper, asfs.ID)     					as func_tick_rn	-- �������������� ������ ��������� �������� �� �������
		, row_number() 
			over(partition by asfs.business_dt, asfs.subbranch_id
					, asfs.ticket_id, asfs.ticket_id_suffix
				order by asfs.next_oper desc, asfs.ID desc)     		as func_tick_rn_desc	-- �������������� ������ ��������� �������� �� �������
		-- ��� �������� ���������� ��������, 
		--    �������� �� �� ���������� ������. ����� �������, ���� �������� 
		--    ������� ���� � ��� �� ���� ��� ����������� �� ������� ������ 
		--    � �������� �����������. ����� ��������� ���������� �������� �
		--    ������ ����� ����� ������ ����� �������� �� ���� ����� � ������.
		, dense_rank()
				over (partition by asfs.business_dt, asfs.subbranch_id
							, asfs.ticket_id, asfs.ticket_id_suffix 
						order by asfs.saphr_id, asfs.function_id, asfs.next_oper, asfs.ID) 			as prec_summ_oper	
	from SBX_RETAIL_SS.vsp_asfs_suo_ALL_ROLES asfs		
	left join "001_mis_retail_channel".vsp_buf_audit_func_dic as dic_func	-- ���������� �������� �� �� ������
		on asfs.function_id = dic_func.id 
	where 0=0
		and asfs.BUSINESS_DT = :rep_dt
	;

	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_asfs_suo_precalc;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_asfs_suo_precalc (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));



	--- --- �� ���������
	-- ������� ��� �������� ���������� �������. ��� ������������ ������ ��������
	set step = 18;
	CALL sbx_retail_ss.sau_DropTable('vt_dist_func');
	
	CREATE MULTISET VOLATILE TABLE vt_dist_func (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		f_name VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		fst_oper VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		lst_oper VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		rn INTEGER,
		cnt_event INTEGER)
	PRIMARY INDEX ( BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix )
	ON COMMIT PRESERVE ROWS;
	
	
	-- ����� ���������� ����������� �������� � ������ ������� ���������� �� �������
	-- ����� ���� � ����������� �������� � ������ ��� ������������ ���������� ������
	insert into vt_dist_func
	select 
		dsuop.business_dt
		, dsuop.subbranch_id
		, dsuop.ticket_id
		, dsuop.ticket_id_suffix
		, dsuop.f_name
		, dsuop.fst_oper
		, dsuop.lst_oper
		, row_number() 
				over(partition by dsuop.business_dt, dsuop.subbranch_id, dsuop.ticket_id, dsuop.ticket_id_suffix
					order by dsuop.pre_rn)     as rn	-- �������������� ���������� �������� ������� �� �������
		, count(*) 
				over(partition by dsuop.business_dt, dsuop.subbranch_id, dsuop.ticket_id, dsuop.ticket_id_suffix) as cnt_event
	from (-- ���������������� �����
		select 
			assu.business_dt
			, assu.subbranch_id
			, assu.ticket_id
			, assu.ticket_id_suffix
			, assu.event_dttm
			, assu.event_type_id
			, assu.POSGR
			, assu.f_name
			, assu.pre_rn
			, assu.func_rn
			, assu.first_oper		as fst_oper
			, assu.last_oper		as lst_oper 
		from vt_asfs_suo_precalc assu
		where 0=0
		    and func_rn = 1  -- ���� ������ ������ ��������� �������
		)dsuop
	;
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_dist_func;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_dist_func (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));



	-- ����� ���������� ������ ��������
	set step = 19;
	CALL sbx_retail_ss.sau_DropTable('vt_oper_list');
	create multiset volatile table vt_oper_list(
		business_dt date
		, subbranch_id integer
		, ticket_id char(4)
		, ticket_id_suffix integer
		, oper_list varchar(3000)
		, fst_oper VARCHAR(500)
		, lst_oper VARCHAR(500)
	)primary index(business_dt, subbranch_id, ticket_id, ticket_id_suffix)
	on commit preserve rows;

	insert into vt_oper_list
	WITH RECURSIVE base (c_business_dt, c_subbranch_id, c_ticket_id, c_ticket_id_suffix
						, f_name, rn, cnt_event, c_list, fst_oper, lst_oper)
	AS
	(
	SELECT 
	    business_dt     	AS c_business_dt
	    , subbranch_id 		AS c_subbranch_id
	    , ticket_id 		AS c_ticket_id 
		, ticket_id_suffix 	AS c_ticket_id_suffix
		, f_name
	    , rn
		, cnt_event
	    , TRIM(CAST(f_name AS VARCHAR(3000))) AS c_list
		, fst_oper
		, lst_oper
	FROM vt_dist_func
	WHERE rn = 1
	UNION ALL
	SELECT 
		c.business_dt     		AS c_business_dt
	    , c.subbranch_id 		AS c_subbranch_id
	    , c.ticket_id 			AS c_ticket_id 
		, c.ticket_id_suffix 	AS c_ticket_id_suffix
		, c.f_name
	    , c.rn
		, c.cnt_event
	    , b.f_name !! ', ' !! TRIM(CAST (c.f_name AS VARCHAR(3000))) 	AS c_list
		, c.fst_oper
		, c.lst_oper
	FROM vt_dist_func c
	JOIN base b
	    ON b.c_business_dt = c.business_dt
	    AND b.c_subbranch_id = c.subbranch_id
		AND b.c_ticket_id = c.ticket_id
		AND b.c_ticket_id_suffix = c.ticket_id_suffix
	    AND b.rn + 1 = c.rn
	)
	SELECT 
		c_business_dt     
	    , c_subbranch_id 	
	    , c_ticket_id 	
		, c_ticket_id_suffix
		, c_list
		, fst_oper
		, lst_oper
	FROM base
	WHERE 
		rn=cnt_event
	;
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_oper_list;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_oper_list (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));




	--- --- �� ����� 
	-- ������� ��� �������� ���������� �������. ��� ������������ ������ �����
	set step = 21;
	CALL sbx_retail_ss.sau_DropTable('vt_dist_posgr');
	
	CREATE MULTISET VOLATILE TABLE vt_dist_posgr (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		POSGR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		rn INTEGER,
		cnt_posgr INTEGER,
		fst_role_manager VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
		scnd_role_manager VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC)
	PRIMARY INDEX ( BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix )
	ON COMMIT PRESERVE ROWS;
	
	insert into vt_dist_posgr
	-- ����� ������������� ����� ��� ������ � ������ ��� ������			
	select 
		dposgr.business_dt
		, dposgr.subbranch_id
		, dposgr.ticket_id
		, dposgr.ticket_id_suffix
		, dposgr.posgr
		, dposgr.rn		-- �������������� ���������� �������� ������� �� �������
		, dposgr.cnt_posgr
		, max(case when rn = 1 then posgr else NULL end)
			over(partition by dposgr.business_dt, dposgr.subbranch_id
					, dposgr.ticket_id, dposgr.ticket_id_suffix
					order by dposgr.rn)		as fst_role_manager
		, max(case when rn = 2 then posgr else NULL end)
			over(partition by dposgr.business_dt, dposgr.subbranch_id
					, dposgr.ticket_id, dposgr.ticket_id_suffix
					order by dposgr.rn)		as scnd_role_manager 
	from (-- ����� ���������� ����������� ����� � ������ ������� ���������� �� �������
		-- ����� ���� � ����������� ����� � ������ ��� ������������ ���������� ������					
		select 
			dsuop.business_dt
			, dsuop.subbranch_id
			, dsuop.ticket_id
			, dsuop.ticket_id_suffix
			, dsuop.posgr
			, row_number() 
					over(partition by dsuop.business_dt, dsuop.subbranch_id, dsuop.ticket_id, dsuop.ticket_id_suffix
						order by dsuop.pre_rn)     as rn	-- �������������� ���������� �������� ������� �� �������
			, count(*) 
					over(partition by dsuop.business_dt, dsuop.subbranch_id, dsuop.ticket_id, dsuop.ticket_id_suffix) as cnt_posgr
		from (-- ���������� ������ ������ ��������� ���� � ������
			select 
				assu.business_dt
				, assu.subbranch_id
				, assu.ticket_id
				, assu.ticket_id_suffix
				, assu.event_dttm
				, assu.event_type_id
				, assu.pre_rn
				, assu.posgr
				, posgr_rn 
			from vt_asfs_suo_precalc assu
			where 0=0
			    and posgr_rn = 1  -- ���� ������ ������ ��������� �������
			)dsuop
		)dposgr
	;
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_dist_posgr;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_dist_posgr (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));


	-- ����� ���������� ������ �����
	set step = 22;
	CALL sbx_retail_ss.sau_DropTable('vt_posgr_list');
	create multiset volatile table vt_posgr_list(
		business_dt date
		, subbranch_id integer
		, ticket_id char(4)
		, ticket_id_suffix integer
		, posgr_list varchar(3000)
		, fst_role_manager varchar(10)
		, scnd_role_manager varchar(10)
	)primary index(business_dt, subbranch_id, ticket_id, ticket_id_suffix)
	on commit preserve rows;

	insert into vt_posgr_list(business_dt, subbranch_id, ticket_id, ticket_id_suffix
						, posgr_list, fst_role_manager, scnd_role_manager)
	WITH RECURSIVE base (c_business_dt, c_subbranch_id, c_ticket_id, c_ticket_id_suffix
						, posgr, rn, cnt_posgr, fst_role_manager, scnd_role_manager, c_list)
	AS
	(
	SELECT 
	    business_dt     	AS c_business_dt
	    , subbranch_id 		AS c_subbranch_id
	    , ticket_id 		AS c_ticket_id 
		, ticket_id_suffix 	AS c_ticket_id_suffix
		, posgr
	    , rn
		, cnt_posgr
		, fst_role_manager
		, scnd_role_manager
	    , TRIM(CAST(posgr AS VARCHAR(3000))) AS c_list
	FROM vt_dist_posgr
	WHERE rn = 1
	UNION ALL
	SELECT 
		c.business_dt     		AS c_business_dt
	    , c.subbranch_id 		AS c_subbranch_id
	    , c.ticket_id 			AS c_ticket_id 
		, c.ticket_id_suffix 	AS c_ticket_id_suffix
		, c.posgr
	    , c.rn
		, c.cnt_posgr
		, c.fst_role_manager
		, c.scnd_role_manager
	    , b.c_list !! ', ' !! TRIM(CAST (c.posgr AS VARCHAR(3000))) 	AS c_list
	FROM vt_dist_posgr c
	JOIN base b
	    ON b.c_business_dt = c.business_dt
	    AND b.c_subbranch_id = c.subbranch_id
		AND b.c_ticket_id = c.ticket_id
		AND b.c_ticket_id_suffix = c.ticket_id_suffix
	    AND b.rn + 1 = c.rn
	)
	SELECT 
		c_business_dt     
	    , c_subbranch_id 	
	    , c_ticket_id 	
		, c_ticket_id_suffix
		, c_list
		, fst_role_manager
		, scnd_role_manager
	FROM base
	WHERE 
		rn=cnt_posgr
		;
		
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_posgr_list;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_posgr_list (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));



	-- ������������ �������������� �������� (�����, ����������, ������������, �����������)
	set step = 23;
	CALL sbx_retail_ss.sau_DropTable('vt_asfs_suo_precalc2');
	
	CREATE MULTISET VOLATILE TABLE vt_asfs_suo_precalc2 (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		client_dk BIGINT,
		first_oper VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		last_oper VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
		summ_oper INTEGER,
		summ_func INTEGER,
		summ_manager INTEGER,
		summ_type_ev INTEGER,
		summ_tema INTEGER,
		min_time_oper TIME(6),
		max_time_oper TIME(6))
	PRIMARY INDEX ( BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix )
	ON COMMIT PRESERVE ROWS;
	
	insert into vt_asfs_suo_precalc2
	select 
	 	BUSINESS_DT
		,SUBBRANCH_ID
		,ticket_id
		,ticket_id_suffix	-- ������� ������(���� ������� ������������)
		,client_dk
		,first_oper	       	-- �������� ������ ��������
		,last_oper			-- �������� ��������� ��������
	    -- ,count(*) 						as summ_oper		-- ���-�� �������� � ������
		--,count(*) 						as summ_oper		-- ���-�� �������� � ������
		,max(prec_summ_oper)			as summ_oper		-- ���-�� �������� � ������
	    ,count(distinct function_id) 	as summ_func 		-- ���-�� �������������� ��������
		,count(distinct saphr_id) 		as summ_manager	    -- ���-�� ���������� ��������� ��������
	    ,count(distinct event_type_id) 	as summ_type_ev 		
	    ,count(distinct CURR_OPCAT_ID) 	as summ_tema
	    ,min(next_oper) 				as min_time_oper 	-- ����� ������ �������� � ������
	    ,max(next_oper) 				as max_time_oper	-- ����� ��������� �������� � ������
	from vt_asfs_suo_precalc		
	group by BUSINESS_DT,SUBBRANCH_ID,ticket_id,ticket_id_suffix,client_dk,first_oper,last_oper
	;
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_asfs_suo_precalc2;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_asfs_suo_precalc2 (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	 
	
	-- ���������� ������� ���������� � ������
	CALL sbx_retail_ss.sau_DropTable('vt_asfs_suo_agg_mng_order');
	create multiset volatile table vt_asfs_suo_agg_mng_order as (
	select distinct
		dsaphrrn.business_dt
		, dsaphrrn.subbranch_id
		, dsaphrrn.ticket_id
		, dsaphrrn.ticket_id_suffix
		--, dsaphrrn.posgr
		, max(case when dsaphrrn.rn = 1 then posgr else NULL end)
			over(partition by dsaphrrn.business_dt, dsaphrrn.subbranch_id
					, dsaphrrn.ticket_id, dsaphrrn.ticket_id_suffix
					order by dsaphrrn.rn)		as fst_role_manager
		, max(case when dsaphrrn.rn = 2 then posgr else NULL end)
			over(partition by dsaphrrn.business_dt, dsaphrrn.subbranch_id
					, dsaphrrn.ticket_id, dsaphrrn.ticket_id_suffix
					order by dsaphrrn.rn)		as scnd_role_manager 
	from (-- ����� ���������� ����������� ����� � ������ ������� ���������� �� �������
		-- ����� ���� � ����������� ����� � ������ ��� ������������ ���������� ������					
		select 
			dsaphr.business_dt
			, dsaphr.subbranch_id
			, dsaphr.ticket_id
			, dsaphr.ticket_id_suffix
			, dsaphr.posgr
			, row_number() 
					over(partition by dsaphr.business_dt, dsaphr.subbranch_id, dsaphr.ticket_id, dsaphr.ticket_id_suffix
						order by dsaphr.event_dttm, dsaphr.ID)     			as rn	-- �������������� ���������� ����� �� �������
		from (-- ���������� ������ ������ ��������� ���� � ������
			select 
				assu.business_dt
				, assu.subbranch_id
				, assu.ticket_id
				, assu.ticket_id_suffix
				, assu.posgr
				, assu.saphr_id
				, assu.event_dttm
				, assu.ID
			from vt_asfs_suo_precalc assu
			where 0=0
			    and saphr_id_rn = 1  -- ���� ������ ������ ��������� ����
			)dsaphr
		)dsaphrrn
	)with data
	primary index(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix)
	on commit preserve rows;



	
	-- ������������ �������������� �������� (�����, ����������, ������������, �����������)
	set step = 24;
	CALL sbx_retail_ss.sau_DropTable('vt_asfs_suo_agg');
	create multiset volatile table vt_asfs_suo_agg (
		BUSINESS_DT 		DATE FORMAT 'YY/MM/DD'
		, SUBBRANCH_ID		BIGINT
		, TICKET_ID			CHAR(4) 
		, ticket_id_suffix	BYTEINT
		, client_dk			BIGINT
		, summ_oper			INTEGER
		, summ_func			INTEGER
		, summ_manager		INTEGER
		, min_time_oper		TIME(6)
		, max_time_oper		TIME(6)
		, fst_role_manager	VARCHAR(255)
		, scnd_role_manager	VARCHAR(255)
		, posgr_list		VARCHAR(3000)	
		, fst_oper			VARCHAR(500)
		, lst_oper			VARCHAR(1000)
		, oper_list			VARCHAR(3000)
	)primary index(business_dt, subbranch_id, ticket_id, ticket_id_suffix)
	on commit preserve rows;

	-- ������������ �������������� �������� (�����, ����������, ������������, �����������)
	insert into vt_asfs_suo_agg
	select 
		pc2.BUSINESS_DT 		
		, pc2.SUBBRANCH_ID		
		, pc2.TICKET_ID			
		, pc2.ticket_id_suffix	
		, pc2.client_dk			
		, pc2.summ_oper			
		, pc2.summ_func			
		, pc2.summ_manager		
		, pc2.min_time_oper		
		, pc2.max_time_oper		
		, mngo.fst_role_manager	
		, mngo.scnd_role_manager	
		, dposl.posgr_list				
		, dopl.fst_oper			
		, dopl.lst_oper			
		, dopl.oper_list		
	from vt_asfs_suo_precalc2 pc2
	left join vt_posgr_list dposl
		on pc2.BUSINESS_DT = dposl.BUSINESS_DT
		and pc2.SUBBRANCH_ID = dposl.SUBBRANCH_ID
		and pc2.TICKET_ID = dposl.TICKET_ID
		and pc2.ticket_id_suffix = dposl.ticket_id_suffix
	left join vt_oper_list dopl
		on pc2.BUSINESS_DT = dopl.BUSINESS_DT
		and pc2.SUBBRANCH_ID = dopl.SUBBRANCH_ID
		and pc2.TICKET_ID = dopl.TICKET_ID
		and pc2.ticket_id_suffix = dopl.ticket_id_suffix
	left join vt_asfs_suo_agg_mng_order mngo
		on pc2.BUSINESS_DT = mngo.BUSINESS_DT
		and pc2.SUBBRANCH_ID = mngo.SUBBRANCH_ID
		and pc2.TICKET_ID = mngo.TICKET_ID
		and pc2.ticket_id_suffix = mngo.ticket_id_suffix
	;
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_asfs_suo_agg;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_asfs_suo_agg (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));




/*****************************************************************************************************************/

-- ����: ������ ���� - ���

/*****************************************************************************************************************/

	CALL sbx_retail_ss.sau_DropTable('vt_asfs1_unique');
	create multiset volatile table vt_asfs1_unique as (
	select distinct BUSINESS_DT, SUBBRANCH_ID, ticket_id, ticket_id_suffix
	from SBX_RETAIL_SS.vsp_asfs_suo_ALL_ROLES asfs1
	where 
		asfs1.BUSINESS_DT = :rep_dt
	)with data
	primary index(BUSINESS_DT, SUBBRANCH_ID, ticket_id, ticket_id_suffix)
	on commit preserve rows;

	collect stats column(BUSINESS_DT, SUBBRANCH_ID, ticket_id, ticket_id_suffix) on vt_asfs1_unique; 





/*****************************************************************************************************************/

-- ����: ������ CRM 

/*****************************************************************************************************************/

------ CRM

	set step = 25;
	-- ������ crm
	CALL sbx_retail_ss.sau_DropTable('oper_crm_mass');
	
	
	SET sql_code = 
	'CREATE MULTISET VOLATILE TABLE oper_crm_mass AS (
	    select 
			tb_id,
			urf_code_actual,
			sap_id,
			client_dk,
			dt_visit,
			login_MP,
			dt_visit_st,
			dt_visit_end,
			CRM_oper,
			suo_tiket_id,
			saphr_id,
			next_oper,
			suo_tiket_id1,
			suo_tiket_id2,
			suo_tiket_id_l,
			ticket_id
	    from foreign table (
			SELECT
				TB_id,  
				urf_code_actual,
				Tab_num       as sap_id, 
				stab_id       as client_dk, 
				DT_VISIT,
				Login_MP,
				Dt_visit_st,
				Dt_visit_end,
				App_type||Current_app_status 	as CRM_oper,
				SUO_TIKET_ID,
				SAPHR_ID,
				dt_visit_st(time) 				as next_oper,
				substr(suo_tiket_id, 1, 1)  					as suo_tiket_id1,
				ltrim(substr(suo_tiket_id, 2, 3)) 				as suo_tiket_id2,
				length(ltrim( substr(suo_tiket_id, 2, 3))) 		as suo_tiket_id_l,
				suo_tiket_id1||right(''000''||suo_tiket_id2, 3)	as ticket_id
			FROM "001_MIS_RETAIL_CHANNEL".VSP_CRM_MASS_DATA_CURRENT
			WHERE DT_VISIT>=DATE ''2020-02-01''
				and dt_visit = date''' || v_char_dt || '''
				
				AND urf_code_actual IS NOT NULL
			    AND Coalesce(activity_type, ''������� �����'') = ''������� �����''
			    AND Coalesce(source_app, ''������� �����������'') = ''������� �����������'' 
	   		)@promtd t
	)
	WITH DATA
	PRIMARY INDEX(sap_id, CRM_oper, client_dk)
	ON COMMIT PRESERVE ROWS'
	;                              
	EXECUTE IMMEDIATE sql_code;


	collect stats column (sap_id, CRM_oper, client_dk) on oper_crm_mass;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'oper_crm_mass (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	

	set step = 26;	-- ��������� ���������� ������� ������
	CALL sbx_retail_ss.sau_DropTable('vt_OPER_CRMM');	
	create multiset volatile table vt_OPER_CRMM as(
	select 
		crm.tb_id,
		crm.urf_code_actual,
		crm.sap_id,
		crm.client_dk,
		crm.dt_visit,
		crm.login_MP,
		crm.dt_visit_st,
		(crm.dt_visit_st + cast (vsph.time_zone as interval hour))  as dt_visit_st2,
		crm.dt_visit_end,
		crm.CRM_oper,
		crm.suo_tiket_id,
		crm.saphr_id,
		crm.next_oper,
		crm.ticket_id,
		vsph.time_zone
	from oper_crm_mass crm
	LEFT JOIN "001_MIS_RETAIL_CHANNEL".VSP_TIME_ZONE_VSP_HIST vsph
		on vsph.urf_code_actual = crm.urf_code_actual
		and crm.dt_visit between vsph.date_start and vsph.date_end
	)with data
	primary index (sap_id, CRM_oper, client_dk)
	on commit preserve rows;

	collect stats column (sap_id, CRM_oper, client_dk) on oper_crm_mass;




/*****************************************************************************************************************/

-- ����: ���������� CRM � ��� 

/*****************************************************************************************************************/

-- help view vt_OPER_CRMM

	set step = 27;
	CALL sbx_retail_ss.sau_DropTable('vt_SUO_CRM');		
	create multiset volatile table vt_SUO_CRM as (
	select
		suo.*,    
		tb_id,
		mp.urf_code_actual 	as urf_code_act,
		sap_id,
		client_dk,
		dt_visit,
		login_MP,
		dt_visit_st,
		dt_visit_end,
		CRM_oper,
		next_oper,
		suo_tiket_id,
		mp.saphr_id 		as sap_crm
	from vt_OPER_CRMM as mp
	join SUO_1 as suo
		on mp.saphr_id = suo.saphr_id
		and mp.dt_visit = suo.business_dt
		and suo.ticket_id = mp.ticket_id
		and not mp.client_dk is null
	)
	with data
	primary index(business_dt, SUBBRANCH_ID, ticket_id, ticket_id_suffix)
	on commit preserve rows
	;
	
	collect stats column (business_dt, SUBBRANCH_ID, ticket_id, ticket_id_suffix) on vt_SUO_CRM;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_SUO_CRM (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));


	set step = 28;	--������� �� SUO_CRM ������ � ����� ��� ����� �������� � ������, ��� ��� �������� �������� CRM_oper is null
	CALL sbx_retail_ss.sau_DropTable('vt_double');
	CREATE MULTISET VOLATILE TABLE vt_double (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT)
	PRIMARY INDEX (BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix)
	ON COMMIT PRESERVE ROWS;

	insert into vt_double 
	select 
		suocrm.business_dt
		, suocrm.subbranch_id
		, suocrm.ticket_id
		, suocrm.ticket_id_suffix
	from vt_SUO_CRM suocrm
	group by suocrm.business_dt
		, suocrm.subbranch_id
		, suocrm.ticket_id
		, suocrm.ticket_id_suffix
	having count(distinct client_dk) > 1
	;

	collect stats column (BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix) on vt_double;


	CALL sbx_retail_ss.sau_DropTable('vt_SUO_CRM_wo_dbl');
	CREATE MULTISET VOLATILE TABLE vt_SUO_CRM_wo_dbl (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		CRM_oper VARCHAR(510) CHARACTER SET UNICODE NOT CASESPECIFIC,
		client_dk BIGINT
		)
	PRIMARY INDEX (BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix)
	ON COMMIT PRESERVE ROWS;

	insert into vt_SUO_CRM_wo_dbl
	select 
	suo.BUSINESS_DT, suo.SUBBRANCH_ID, suo.TICKET_ID, suo.ticket_id_suffix, suo.crm_oper, suo.client_dk
	from vt_SUO_CRM suo
	left join vt_double dbl
		on suo.BUSINESS_DT=dbl.BUSINESS_DT
		and suo.SUBBRANCH_ID=dbl.SUBBRANCH_ID
		and suo.ticket_id=dbl.ticket_id
		and suo.ticket_id_suffix=dbl.ticket_id_suffix
	where 
		suo.CRM_oper IS NOT NULL
		and dbl.business_dt IS NULL	-- ���������� ������
	;
	
	collect stats column (BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix) on vt_SUO_CRM_wo_dbl;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_SUO_CRM_wo_dbl (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));



/*****************************************************************************************************************/

-- ����: ������ ������ CRM

/*****************************************************************************************************************/



	set step = 29;	-- �������� �� ��������� � CRM ��� ���������� � ��� ��������� ������
	CALL sbx_retail_ss.sau_DropTable('vt_suo_crm_agg');
	CREATE MULTISET VOLATILE TABLE vt_suo_crm_agg (
		BUSINESS_DT DATE FORMAT 'YY/MM/DD',
		SUBBRANCH_ID BIGINT,
		TICKET_ID CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
		ticket_id_suffix BYTEINT,
		client_dk BIGINT,
		cnt INTEGER,
		cntd INTEGER)
	PRIMARY INDEX ( BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix )
	ON COMMIT PRESERVE ROWS;

	insert into vt_suo_crm_agg
	select BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix, client_dk, count(1) as cnt, count(distinct crm_oper) as cntd
	from vt_SUO_CRM_wo_dbl
	group by BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix, client_dk
	;
	
	collect stats column (BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix) on vt_suo_crm_agg;

	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_suo_crm_agg (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));

	
/*****************************************************************************************************************/

-- ����: ������ CRM ��� ����

/*****************************************************************************************************************/

	set step = 30 ;	-- ������ CRM, ������� ��� � ����
	CALL sbx_retail_ss.sau_DropTable('vt_suo_crm_wo_dbl_wo_asfs');
	create multiset volatile table vt_suo_crm_wo_dbl_wo_asfs as (
	select
		crm.BUSINESS_DT, crm.SUBBRANCH_ID, crm.TICKET_ID, crm.ticket_id_suffix, crm.client_dk
	from 
		-- vt_SUO_CRM_wo_dbl crm
		vt_suo_crm_agg crm
	left join vt_asfs1_unique tab1		
		on tab1.BUSINESS_DT = crm.BUSINESS_DT
		and tab1.SUBBRANCH_ID = crm.SUBBRANCH_ID
		and tab1.TICKET_ID = crm.TICKET_ID
		and tab1.ticket_id_suffix = crm.ticket_id_suffix
	where 
		tab1.business_dt IS NULL		
	)with data
	primary index(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix)
	on commit preserve rows;
	
	collect stats column (BUSINESS_DT ,SUBBRANCH_ID ,TICKET_ID ,ticket_id_suffix) on vt_suo_crm_wo_dbl_wo_asfs;


	
		
/*****************************************************************************************************************/

-- ����: ����������� ������� ���� � CRM 

/*****************************************************************************************************************/

	
	-- ��� ���������� ������� �� ���� � CRM
	CALL sbx_retail_ss.sau_DropTable('vt_ticket_all');
	create multiset volatile table vt_ticket_all as (
	select crm.BUSINESS_DT, crm.SUBBRANCH_ID, crm.TICKET_ID, crm.ticket_id_suffix
	from vt_suo_crm_wo_dbl_wo_asfs crm
	union all
	select crm.BUSINESS_DT, crm.SUBBRANCH_ID, crm.TICKET_ID, crm.ticket_id_suffix 
	from vt_asfs1_unique crm
	where 
		crm.BUSINESS_DT = :rep_dt
		-- crm.BUSINESS_DT = date'2020-06-16'
	)with data
	primary index(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix)
	on commit preserve rows
	;




	
/*****************************************************************************************************************/

-- ����: ���� ��������� �������

/*****************************************************************************************************************/



	-- ������� ��� ���������� ����������
	CALL sbx_retail_ss.sau_DropTable('vt_result');
	create multiset volatile table vt_result as (
	select
		coalesce(asfs.client_dk, crm.client_dk)		as client_dk
		, tick.BUSINESS_DT
		, tick.SUBBRANCH_ID
		, tick.TICKET_ID
		, tick.ticket_id_suffix
		, suo.n_emploee				-- 
		, n_events
		, n_temas
		, time_get_ticket
		, fst_ticket_tema
		, time_call_ticket
		, cnt_event
		, cntd_event
		, event_list
		, n_calls
		, new_TICKET_WAIT_TIME
		, sum_ticket_wait_time
		, max_TICKET_WAIT_TIME
		, time_before_close
		, n_counter_change
		, sum_ticket_delay_flg
		, max_TICKET_DROP_FLG
		, max_TICKET_SERV_TIME
		, avg_TICKET_SERV_TIME
		, role_list_suo
		, asfs.summ_oper			-- 
		, summ_func
		, summ_manager
		, min_time_oper
		, max_time_oper
		, fst_role_manager
		, scnd_role_manager
		, posgr_list
		, fst_oper
		, lst_oper
		, oper_list
		, crm.cntd			as n_act_crm
	from vt_ticket_all tick		-- ��� ���������� ������ �� ���� � CRM
	inner join vt_suo_agg suo 	-- � ������� ������������ ������������ �������� �� ���
		on tick.BUSINESS_DT = suo.BUSINESS_DT
		and tick.SUBBRANCH_ID = suo.SUBBRANCH_ID
		and tick.TICKET_ID = suo.TICKET_ID
		and tick.ticket_id_suffix = suo.ticket_id_suffix
	left join vt_asfs_suo_agg asfs	-- ������� � ��������� �� ���� + ���   
		on tick.BUSINESS_DT = asfs.BUSINESS_DT
		and tick.SUBBRANCH_ID = asfs.SUBBRANCH_ID
		and tick.TICKET_ID = asfs.TICKET_ID
		and tick.ticket_id_suffix = asfs.ticket_id_suffix
	left join vt_suo_crm_agg crm	-- ������������ ������ �� CRM (���������� ����������� CRM) 
		on tick.BUSINESS_DT = crm.BUSINESS_DT
		and tick.SUBBRANCH_ID = crm.SUBBRANCH_ID
		and tick.TICKET_ID = crm.TICKET_ID
		and tick.ticket_id_suffix = crm.ticket_id_suffix
	)with data
	primary index(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix)
	on commit preserve rows;
	
	
	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, 'vt_result (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));
	
	collect stats column(BUSINESS_DT, SUBBRANCH_ID, TICKET_ID, ticket_id_suffix) on vt_result;
	
	
	
	-- ��� �������� �������
	set step = 31;
	delete from SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1  where BUSINESS_DT = :rep_dt; 	
	insert into SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1  (
			BUSINESS_DT,
			SUBBRANCH_ID,
			TICKET_ID,
			ticket_id_suffix,
			client_dk,
			summ_oper,
			summ_func,
			summ_manager,
			min_time_oper,
			max_time_oper,
			fst_role_manager,
			scnd_role_manager,
			posgr_list,		
			oper_list,		
			fst_oper,
			lst_oper,
			n_emploee,
			n_events,
			n_temas,
			time_get_ticket,
			fst_ticket_tema,
			time_call_ticket,
			cnt_event,
			cntd_event,
			event_list,		 
			n_calls,
			new_TICKET_WAIT_TIME,
			sum_ticket_wait_time,
			max_TICKET_WAIT_TIME,
			time_before_close,
			n_counter_change,
			sum_ticket_delay_flg,
			max_TICKET_DROP_FLG,
			max_TICKET_SERV_TIME,
			avg_TICKET_SERV_TIME,
			n_act_crm,
			role_list_suo)
	select 
		res.BUSINESS_DT,
		res.SUBBRANCH_ID,
		res.TICKET_ID,
		res.ticket_id_suffix,
		res.client_dk,
		res.summ_oper,
		res.summ_func,
		res.summ_manager,
		res.min_time_oper,
		res.max_time_oper,
		res.fst_role_manager,
		res.scnd_role_manager,
		res.posgr_list,		
		res.oper_list,		
		res.fst_oper,
		res.lst_oper,
		res.n_emploee,
		res.n_events,
		res.n_temas,
		res.time_get_ticket,
		res.fst_ticket_tema,
		res.time_call_ticket,
		res.cnt_event,
		res.cntd_event,
		res.event_list,		 
		res.n_calls,
		res.new_TICKET_WAIT_TIME,
		res.sum_ticket_wait_time,
		res.max_TICKET_WAIT_TIME,
		res.time_before_close,
		res.n_counter_change,
		res.sum_ticket_delay_flg,
		res.max_TICKET_DROP_FLG,
		res.max_TICKET_SERV_TIME,
		res.avg_TICKET_SERV_TIME, 
		res.n_act_crm,
		res.role_list_suo
	from vt_result res
	;
	
	DELETE FROM SBX_RETAIL_SS.vsp_asfs_suo_oper WHERE BUSINESS_DT = :rep_dt;
	DELETE FROM SBX_RETAIL_SS.vsp_asfs_suo_ALL_ROLES WHERE BUSINESS_DT = :rep_dt;
	
	call SBX_RETAIL_SS.dq_vsp_visit(:rep_dt);

	set step = 32;	
	insert into SBX_RETAIL_SS.sau_test_log(target_table, proc_id, step, err_msg, cnt_rows, dtm)
	values ('SBX_RETAIL_SS.vsp_asfs_suo_ticket_tab1 ', :v_proc_id, :step, '�����. (' || v_char_dt || ')', ACTIVITY_COUNT, current_timestamp(0));

end;