DECLARE p_from_date, p_to_date , p_reporting_date DATE;
DECLARE p_year, p_week, p_period, ytd_week INT64;

DECLARE p_cur_year, p_prev_year, p_prev_year2 INT64;

DECLARE dynamic_sql STRING;

SET p_cur_year = (select extract (YEAR from current_date() ));

SET p_prev_year=p_cur_year-1;
set p_prev_year2=p_cur_year-2;

SET (p_from_date, p_to_date , p_reporting_date) = (select as VALUE STRUCT (max(date(report_effective_date))-6, max(date(report_effective_date)), max(date(report_effective_date)))
                                                   from ppl_analytics_output_cfs.workday_weekly_headcount
                                                  );


--SET (p_from_date, p_to_date , p_reporting_date) = ('2024-10-20', '2024-10-26', '2024-10-26');

SET (p_year, p_week, p_period) = (select as VALUE STRUCT (yr_num, wk_of_yr_num, pd_of_yr_num)
                                  from ppl_analytics_output_cfs.Corporate_Calendar
                                  where cal_dt=p_to_date);

-- update status table

----------------------------------------------------------------
-- STEP 1: source headcount data prep for last week
----------------------------------------------------------------
create or replace table airflow_test.latest_week_headcount_v3 as
select distinct
p_year as year,
p_week as week,
cost_center_id,
time_type,
shift_type,
category,
gfw,
three_months,
first_year,
employee_id,
on_leave,
hire_date,
pay_rate_type,
job_category
--hc2.*, dc_city.dc_city
from
(SELECT
  hc1.*,
  case when tenure_days <= 90 then 'yes'
         else 'no' 
  end as three_months,
  case when tenure_days <= 365 then 'yes'
         else 'no' 
  end as first_year,
  ifnull (work_shift, 'Not applicable') as shift_type, 
  case when category=1 then 'Grocery'
       when category in (2, 5,6,7,8,9,0) then 'Perishable'
       when category = 3 then 'Repack'
       when category = 4 then 'Frozen'
       else 'Not applicable'
  end as category, 
  case when lower(gender)<> 'b) man/boy' then 'yes' else 'no' end as gfw
FROM (
  SELECT 
    employee_id,
    employee_type,
    time_type,
    active_status,
    on_leave,
    company,
    job_profile,
    city,
    gender,
    cch_level_03,
    cch_level_04,
    store_number,
    cost_center_id,
    cost_center_name,
    cost_center,
    report_effective_date,
    store_support_colleague,
    tenure_category,
    hire_date,
    extract (day from current_date() - date(hire_date)) tenure_days,
    hc.rec_cre_tms,
    location,
    --trim(substr(work_shift, 1, instr(work_shift , '/')-1)) work_shift
    trim(replace (work_shift, '(Canada)', '')) work_shift,
    pay_rate_type,
    job_category
  FROM
    ppl_analytics_output_cfs.workday_weekly_headcount hc
  WHERE
    LOWER(cch_level_03)='distribution center'
    --AND LOWER(employee_type)= 'regular'
    AND LOWER(active_status)= 'yes'
    AND store_support_colleague IS NULL
    and date(report_effective_date)=p_reporting_date) hc1
LEFT JOIN (
  SELECT
    DISTINCT emple_id,
    --lbr_cat_shft_desc shift_type,
    safe_cast (substring (split(bus_strc_path_desc, '/')[array_length(split(bus_strc_path_desc, '/'))-2], 1,1) as int) as category,
    dense_rank() over (partition by emple_id order by appl_dt desc) rn
  FROM
    ppl_analytics_output_cfs.kronos_paycode pc
  WHERE DATE(appl_dt) between p_from_date and p_to_date) kronos
ON
  hc1.employee_id=kronos.emple_id and
  kronos.rn=1) hc2;