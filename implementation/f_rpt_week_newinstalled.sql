drop function f_rpt_week_newinstalled;

create or replace function f_rpt_week_newinstalled
(
		param_company_id text
		, param_loc_id text
		, param_start_date text
		, param_end_date text
		, param_view_name text
		, param_view_name_raw text
)
RETURNS numeric 
AS $BODY$	
DECLARE 
	query1 text; query2 text; query3 text;
	flag_result numeric;

BEGIN
	flag_result = 0; -- default failed

	query1 = '
			select
				pat.pattern
				, ww.start_date
				, (

						select
							count(distinct(a.tyre_sn)) cnt_new_installed
						from t_tyre_hist a
						left join t_tyre b on (b.tyre_id = a.tyre_id)
						where
							a.company_id = '''||param_company_id||''' 
							and (
										case
											when '''' = '''' then true
											else a.loc_id = '''||param_loc_id||'''
										end
									)
							and a.job_status = 1 
							and a.disposition = ''RUNNING''
							and a.is_new_installed = 1
							and a.job_date = ww.start_date
							and b.tyre_pattern_id = pat.pattern
							and a.tyre_sn is not null
				  ) as cnt_new_installed
			from
			(
				select ''-'' as pattern
				union
				select
					distinct b.tyre_pattern_id as pattern
				from t_tyre_hist a
				inner join t_tyre b on (b.tyre_id = a.tyre_id)
				where
					a.company_id = '''||param_company_id||''' 
					and (
								case
									when '''' = '''' then true
									else a.loc_id = '''||param_loc_id||'''
								end
							)
					and a.job_status = 1 
					and a.disposition = ''RUNNING''
					and a.is_new_installed = 1	
					and (a.job_date between '''||param_start_date||''' and '''||param_end_date||''')
			) as pat, 
			(
				select d::date start_date
				from generate_series('''||param_start_date||'''::date,'''||param_end_date||''', ''1 day'') d
			) ww
						 ';
	
	query2 =        '
	select d::date start_date
	from generate_series('''||param_start_date||'''::date,'''||param_end_date||''', ''1 day'') d
	order by d::date
			 ';

						
	query3 = concat('create or replace temp view ',param_view_name_raw,' as ',query1);
	execute query3;

	execute dynamic_pivot_simple(query1,query2,param_view_name,'tv');
	
	flag_result = 1; -- default failed


	RETURN flag_result;
END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100

select f_rpt_week_newinstalled('24910','','2021-04-13','2021-04-19','viewx_crosstab','viewx_ori');

-- select f_rpt_week_newinstalled('24910','','2021-03-04','2021-03-10','viewx_crosstab','viewx_ori');

select * from viewx_ori;
select * from viewx_crosstab;

drop view if exists viewx_ori;
drop view if exists viewx_crosstab;

