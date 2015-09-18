/* very quick test run of stratification with all automation stripped out */

/*	Get volumes required - note need to clarify significance rules as chi sq values seem very low in test data*/
drop view rcb_req_ppl cascade;
create view rcb_req_ppl as
select 
      exposed,
	  first_purchase_date,
      acorn_group as v1,
      behavioural_segment_code as v2,
      age_band_code as v3,
      pre_prod1_spend_ntile as v4,
      my_shopping_3_code as v5,
      count(customer_key) as ppl_req
from RKRISCIUNAITE.cust_vars_1
where exposed=1
group by 1,2,3,4,5,6,7;      

--select * from rcb_req_ppl;
select count(exposed) from rcb_req_ppl; --4124

/*	select control matches*/
create view  ctrl_group_1 as 
    select 
        customer_key
    from
      rcb_req_ppl c inner join
      (select 
       cd.customer_key,
	   cd.first_purchase_date,
       cd.acorn_group as v1,
       cd.behavioural_segment_code as v2,
        age_band_code as v3,
        pre_prod1_spend_ntile as v4,
        my_shopping_3_code as v5,
        row_number() over (partition by first_purchase_date, acorn_group, behavioural_segment_code, age_band_code,
                          pre_prod1_spend_ntile, my_shopping_3_code /*order by customer_key*/) as rn
      from
      RKRISCIUNAITE.cust_vars_1 cd ) r
      on c.first_purchase_date = r.first_purchase_date and c.v1=r.v1 and c.v2=r.v2 and c.v3=r.v3 and c.v4=r.v4 and c.v5=r.v5 
      where rn <= ppl_req;
select count(customer_key) from ctrl_group_1; --12408
	  
	  
/* create long file with stratification variables & values*/	  
drop view ctrl_stats_long ;
create view ctrl_stats_long as select
     1 as ANALYSIS_ROW_NUM,
      'test' as PROD1_NAME,
       cl.exposed,
       cl.customer_key,
       cl.location,
      cl.FIRST_PURCHASE_DATE,
      cl.var_name,
      case when cl.var_name =  'acorn_group' then 1
          when cl.var_name =  'behavioural_segment_code' then 2
          when cl.var_name =  'age_band_code' then 3
          when cl.var_name =  'pre_prod1_spend_ntile' then 4
          when cl.var_name =  'my_shopping_3_code' then 5
          end as var_num,
      cl.var_val as STRAT_VAR_VAL

from
    RKRISCIUNAITE.CUST_stats_long_1 cl inner join ctrl_group_1 ct on 
    cl.customer_key=ct.customer_key
    where cl.var_name in ('acorn_group','behavioural_segment_code','age_band_code','pre_prod1_spend_ntile','my_shopping_3_code')
;    
select count(customer_key) from ctrl_stats_long; --54120

/* Append uplift variables to long file*/
--drop view ctrl_stats_long_metrics1;
create view ctrl_stats_long_metrics1 as select 
      csl.*,
      cs.pre_prod1_spend,
      cs.pre_prod2_spend,
      cs.pre_prod1_units,
      cs.pre_prod2_units,
      cs.pre_prod1_visits,
      cs.pre_prod2_visits
     
from
           ctrl_stats_long csl inner join
           RKRISCIUNAITE.cust_stats_1 cs
on  csl.customer_key=cs.customer_key  and csl.location=cs.location ;      
select count(customer_key) from ctrl_stats_long_metrics1; --54120
--