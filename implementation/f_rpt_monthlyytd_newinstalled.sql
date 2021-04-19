drop function if exists f_rpt_monthlyytd_newinstalled;

create or replace function f_rpt_monthlyytd_newinstalled
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
	
	v_end_year_ago date; -- des 01, tahun lalu
	v_start_year date; -- jan 01, tahun ini
	
	v_min_date date; -- tgl awal
	v_max_date date; -- tgl akhir
	

BEGIN
	flag_result = 0; -- default failed
	select extract('year' from param_end_date::date)-1||'-12-01' into  v_end_year_ago;
	select extract('year' from param_end_date::date)||'-01-01' into  v_start_year;
	
	select (param_start_date::date+6) into v_max_date;
	
	select min(ww.start_date) into v_min_date 
	from 
	(
	select d::date-6 as start_date, d::date as end_date
	from generate_series(param_start_date::date,v_end_year_ago, '-7d') d
	) ww
	where ww.end_date >= v_start_year;	
	
	query1 = '

						select
							pat.pattern
							, ww.months
							,(
								select
									count(distinct a.tyre_sn) cnt_new_installed
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
									and to_char(a.job_date,''YYYY-MM'')= ww.months
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
								and (a.job_date between '''||v_start_year||''' and '''||v_max_date||''')
						) as pat, 
						(
							select to_char(d::date,''YYYY-MM'') as months
							from generate_series('''||v_start_year||'''::date,'''||v_max_date||''', ''1month'') d
							order by to_char(d::date,''YYYY-MM'')			
						) ww
						group by pat.pattern, ww.months
						 ';
	
	query2 =	'
							select to_char(d::date,''YYYY-MM'') as months
							from generate_series('''||v_start_year||'''::date,'''||v_max_date||''', ''1month'') d
							order by d::date
						';
						
	query3 = concat('create or replace temp view ',param_view_name_raw,' as ',query1);
	execute query3;
	 
	execute dynamic_pivot_simple(query1,query2,param_view_name,'tv');

	RETURN flag_result;
END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100

-- cara menggunakan 
select f_rpt_monthlyytd_newinstalled('24910','','2021-03-04','2021-03-10','viewx782398kduiwlkjdo94k','viewx782398kduiwlkjdo94k_ori');

select * from viewx782398kduiwlkjdo94k;
select * from viewx782398kduiwlkjdo94k_ori;

drop view if exists viewx782398kduiwlkjdo94k_ori;
drop view if exists viewx782398kduiwlkjdo94k;
