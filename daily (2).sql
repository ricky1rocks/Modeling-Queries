truncate table analytics_pricing.product_list;
insert into analytics_pricing.product_list 
(
       select distinct fso.product_sid, fso.vendor_sid
       from dwh.f_suborders_oms_vw fso
       where to_date(fso.subo_date_verified::varchar,'YYYYMMDD')>='2021-11-01'--  between '2021-11-24' and '2021-11-25'
       union 
       select distinct product_sid, vendor_sid 
       from analytics_pricing.cash_back_final_update_supc_vendorcode_backup where is_pushed=1
       union 
       select distinct product_sid, vendor_sid 
       from analytics_pricing.cash_back_final_update_supc_vendorcode where  is_pushed=1
	   union 
       select distinct vip.product_sid, vendor_sid 
       from  dwh.f_vendor_inventory_pricing vip
       join dwh.d_product dp on dp.product_sid=vip.product_sid
       where product_offer_group_id in ('665623976236', '644786366417', '648691312362', '685813730061', '667529279951', '641435858807', '680751976490', '627533524527', '669598884046', '680751976490', '648691312362', '622580835565', '676670152662', '684425513522', '645583425082', '655675775651', '646780230676', '637282530735', '682691399955', '667124163314', '667660002401', '681462338256', '621103626657', '672253273222', '674270878706', '640889445600', '625422573847', '625422573847', '640838423119', '621638292644', '673685613200', '672439055378', '676181668294')
);

-----7,9,12,16,19,21

TRUNCATE TABLE  analytics_pricing.DataupdateSteps;

INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step1',(SELECT NOW()),NULL;

TRUNCATE TABLE analytics_pricing.fso_table;
insert into analytics_pricing.fso_table
select distinct fso.product_sid,fso.vendor_sid,
first_value(fso.closing_fee) over(partition by fso.product_sid,fso.vendor_sid order by fso.subo_date_verified DESC,fso.subo_date_created DESC,fso.suborder_code DESC) as 'closing_fee',
first_value(zeroifnull(sfp1.value)) over(partition by fso.product_sid,fso.vendor_sid order by fso.subo_date_verified DESC,fso.subo_date_created DESC,fso.suborder_code DESC) as 'RTO_Logistic_charge',
first_value(zeroifnull(sfp2.value)) over(partition by fso.product_sid,fso.vendor_sid order by fso.subo_date_verified DESC,fso.subo_date_created DESC,fso.suborder_code DESC) as 'RPR_Logistic_charge',
first_value(fso.sf_fulfillment_charges) over(partition by fso.product_sid,fso.vendor_sid order by fso.subo_date_verified DESC,fso.subo_date_created DESC,fso.suborder_code DESC) as 'fulfillment_charges'
from dwh.f_suborders_oms_vw fso
left join snapdeal_ops_dwh.suborder_financial_parameter sfp1 on sfp1.suborder_code = fso.suborder_code and sfp1.key = 'REVERSE_LOGISTICS_CHARGES_RTO_FORWARD'
left join snapdeal_ops_dwh.suborder_financial_parameter sfp2 on sfp2.suborder_code = fso.suborder_code and sfp2.key = 'REVERSE_LOGISTICS_CHARGES_RPR_FORWARD'
where to_date(subo_date_verified::varchar,'YYYYMMDD')>=current_date-30
;

drop table if exists pog_sale_30;
create local temp table pog_sale_30 on commit preserve rows as 
(
select dp.product_offer_group_id as pog
, 1 as Sold_30_flag
, max(case when to_date(fso.subo_date_verified::varchar,'YYYYMMDD') between current_date-7 and current_date-1 then 1 else 0 end) as Sold_7_flag
, count(distinct suborder_code) as last_30_day_pog_sale
, count(distinct case when to_date(fso.subo_date_verified::varchar,'YYYYMMDD') between current_date-7 and current_date-1 then suborder_code else null end) as last_7_day_pog_sale
from dwh.f_suborders_oms_vw fso
left join dwh.d_product dp on fso.product_sid = dp.product_sid
where to_date(fso.subo_date_verified::varchar,'YYYYMMDD') between current_date-30 and current_date-1
group by 1,2
);

drop table if exists seller_supc_sale_D15;
create local temp table seller_supc_sale_D15 on commit preserve rows as 
(
select fso.product_sid, fso.vendor_sid, 
count(distinct suborder_code) as 'lastD15Sale' 
, count(distinct case when to_date(fso.subo_date_verified::varchar,'YYYYMMDD') between current_date-7 and current_date-1 then suborder_code else null end) as lastD7Sale
                        from dwh.f_suborders_oms_vw fso 
                        where TO_DATE(fso.subo_date_verified::varchar,'YYYYMMDD') BETWEEN CURRENT_DATE - 15 AND CURRENT_DATE - 1 
                        group by 1,2
);

drop table if exists vendor;
create local temp table vendor on commit preserve rows as 
select * from (
select distinct vendor_sid,vendor_code,
count(supc||vendor_code) as acount,
count(case when vip.enabled = 1 and vip.enabled_by_seller=1 and vip.signature_present = 1 and vip.gst_enabled = 1 and inventory - inventory_sold>0 then vip.supc else null end) as 'enabled1'
from dwh.f_vendor_inventory_pricing vip
group by 1,2)a
where enabled1<3000 or enabled1 between 3000 and 25000;


drop table if exists vendor1;
create local temp table vendor1 on commit preserve rows as 
select distinct vendor_sid from 
(
select ROW_NUMBER() OVER(ORDER BY Subos_D1 DESC) AS Rownum,* from 
(
        select 
        fso.vendor_sid,count(distinct fso.suborder_code) as Subos_D1
        from dwh.f_suborders_oms_vw fso
        join vendor v on v.vendor_sid=fso.vendor_sid
        where to_date(fso.subo_date_verified::varchar,'YYYYMMDD') >=current_date-15
        and enabled1<25000 
        group by 1
        order by 2 desc
)a
)a
where Rownum<=3000;

delete from analytics_pricing.today_sp_change_supc_vendor where date(aud_create_ts)=current_date-1;

select refresh('analytics_pricing.recalculated_financial_dump_today');

--select refresh('analytics_pricing.subcat_min_gm_cutoff');

--SELECT ANALYZE_STATISTICS('analytics_pricing.recalculated_financial_dump_today');


truncate table analytics_pricing.recalculated_financial_dump_today;
insert into analytics_pricing.recalculated_financial_dump_today
select * from snapdeal_ops_dwh.recalculated_financial
;

update analytics_pricing.recalculated_financial_dump_today a
set selling_price=b.selling_price,
seller_price=b.seller_price,
fixed_margin_amount=b.fixed_margin_amount,
fulfillment_charges=b.fulfillment_charges,
logistics_cost=b.logistics_cost,
closing_fee=b.closing_fee,
payment_collection_charges=b.payment_collection_charges,	
reverse_logistics_charges_rto_forward=b.reverse_logistics_charges_rto_forward,	
reverse_logistics_charges_rpr_forward=b.reverse_logistics_charges_rpr_forward
from analytics_pricing.today_sp_change_supc_vendor b
where lower(a.supc)=lower(b.supc) and lower(a.seller_code)=lower(b.vendor_code)
and a.selling_price<>b.selling_price;



UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step1';

INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step2',(SELECT NOW()),NULL;

TRUNCATE TABLE analytics_pricing.base_tables1;
insert into analytics_pricing.base_tables1 
select vip.supc, vip.vendor_code, vip.product_sid, vip.vendor_sid, dp.product_offer_group_id as 'pog_id',bucket_id
	, dp.subcategory_id, dp.subcategory_name, dp.category_id, dp.category_name, dp.new_supercategory,dp.name as 'Product_name'
	, dp.brand_name,dp.brand_id
	, vip.selling_price as vip_selling_price
	, rf.selling_price
	, rf.seller_price
	, rf.oneship_charges
	, rf.fixed_margin_amount
	, ifnull(fso.fulfillment_charges,rf.fulfillment_charges) as fulfillment_charges
	, rf.logistics_cost
	, ifnull(fso.closing_fee,rf.closing_fee) as closing_fee
	, rf.payment_collection_charges
	, ifnull(fso.RTO_Logistic_charge,(case when rf.reverse_logistics_charges_rto_forward = rf.logistics_cost then 0 else rf.reverse_logistics_charges_rto_forward end)) as 'reverse_logistics_charges_rto_forward'
	, ifnull(fso.RPR_Logistic_charge,rf.reverse_logistics_charges_rpr_forward) as reverse_logistics_charges_rpr_forward
	, CEIL(GREATEST(dp.pa_weight::float,(dp.pa_length::float*dp.pa_breadth::float*dp.pa_height::float/5))/500) as 'slab'
	, (rf.selling_price-rf.seller_price)/1.18 as 'sd_share_pre_cashback'
        ,case when vip.gst_enabled = 1 and vip.signature_present = 1 and vip.block_inventory_enabled = 1 and vip.enabled_by_seller = 1 and vip.enabled = 1 then 1 else 0 end  as 'is_enabled'
        ,case when vip.gst_enabled = 1 and vip.signature_present = 1 and vip.block_inventory_enabled = 1 AND vip.enabled_by_seller = 1 and vip.enabled = 1 and  inventory > inventory_sold then 1 else 0 end as 'is_enabled_with_inv'
        , 0 as 'cashback'      
        , ifnull(vip.int_cashback_snapdeal_component,0) as 'vip_cashback'
        ,vip.updated as vip_updated       
         ,ifnull(sale2.Sold_7_flag,0) as Sold_7_flag_pog
         ,ifnull(sale2.Sold_30_flag,0) as Sold_30_flag_pog	
         ,ifnull(sale2.last_7_day_pog_sale,0) as last_7_day_pog_sale
	 ,ifnull(sale2.last_30_day_pog_sale,0) as last_30_day_pog_sale
         ,ifnull(sale1.lastD7Sale,0) as lastD7Sale
	 ,ifnull(sale1.lastD15Sale,0) as lastD15Sale

	 ,case when rf.selling_price<=200 then 'a_0 to 200'
              when rf.selling_price<=400 then 'b_200 to 400'
              when rf.selling_price<=600 then 'c_400 to 600'
              when rf.selling_price<=800 then 'd_600 to 800'
              when rf.selling_price<=1000 then 'e_800 to 1000'
              when rf.selling_price>1000 then 'f_1000+'
              else 'unknown' end as priceBucket    	  
	from dwh.f_vendor_inventory_pricing  vip
	join analytics_pricing.recalculated_financial_dump_today rf on rf.supc = vip.supc and vip.vendor_code = rf.seller_code
	join dwh.d_product dp on vip.product_sid = dp.product_sid
	left join analytics_pricing.fso_table fso on fso.vendor_sid=vip.vendor_sid and fso.product_sid=vip.product_sid
	left join seller_supc_sale_D15 as sale1 on sale1.product_sid = vip.product_sid and sale1.vendor_sid = vip.vendor_sid
	left join pog_sale_30 sale2 on sale2.pog= dp.product_offer_group_id 	
	join analytics_pricing.product_list fc on vip.vendor_sid =fc.vendor_sid and vip.product_sid =fc.product_sid				
--where vip.supc='SDL397535245'
;

UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step2';

INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step3',(SELECT NOW()),NULL;

DROP TABLE IF EXISTS subcategoryId_exception_list;
CREATE LOCAL TEMP TABLE subcategoryId_exception_list on commit preserve rows as
select *
from
(
select *, 
row_number()over(partition by subcategory_id order by updated_date desc,is_exception desc,hard_exception desc) as ranks
from analytics_pricing.subcategoryId_exception_list rf
)a
where ranks=1;

DROP TABLE IF EXISTS subcategoryId_vendorCode_exception_list;
CREATE LOCAL TEMP TABLE subcategoryId_vendorCode_exception_list on commit preserve rows as
select *
from
(
select subcategory_id,
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
is_exception,hard_exception,updated_date,
row_number()over(partition by subcategory_id,vendor_code order by updated_date desc,is_exception desc,hard_exception desc) as ranks
from analytics_pricing.subcategoryId_vendorCode_exception_list rf
)a
where ranks=1;

DROP TABLE IF EXISTS brandId_brandname_exception_list;
CREATE LOCAL TEMP TABLE brandId_brandname_exception_list on commit preserve rows as
select *
from
(
select brand_id,brand_name,is_exception,hard_exception,updated_date,
row_number()over(partition by brand_id,brand_name order by updated_date desc,is_exception desc,hard_exception desc) as ranks
from analytics_pricing.brandId_brandname_exception_list rf
)a
where ranks=1;


DROP TABLE IF EXISTS supc_vendorcode_exception_list;
CREATE LOCAL TEMP TABLE supc_vendorcode_exception_list on commit preserve rows as
select *
from
(
select 
case when substring(supc, 1, 1)='sdl' then initcap(supc) else supc end as supc,
case when substring(vendorcode, 1, 1)='s' then initcap(vendorcode) else vendorcode end as vendorcode,
is_exception,hard_exception,updated_date,
row_number()over(partition by supc,vendorcode order by updated_date desc,is_exception desc,hard_exception desc) as ranks
from analytics_pricing.supc_vendorcode_exception_list rf
)a
where ranks=1;

DROP TABLE IF EXISTS supc_exception_list;
CREATE LOCAL TEMP TABLE supc_exception_list on commit preserve rows as
select *
from
(
select case when substring(supc, 1, 1)='sdl' then initcap(supc) else supc end as supc,
is_exception,hard_exception,updated_date,
row_number()over(partition by supc order by updated_date desc,is_exception desc,hard_exception desc) as ranks
from analytics_pricing.supc_exception_list rf
)a
where ranks=1;

DROP TABLE IF EXISTS vendorcode_exception_list;
CREATE LOCAL TEMP TABLE vendorcode_exception_list on commit preserve rows as
select *
from
(
select 
case when substring(vendorcode, 1, 1)='s' then initcap(vendorcode) else vendorcode end as vendorcode,
is_exception,
/*case when updated_by in ('kshitij.srivastava@snapdeal.com','ashwani.kumar03@snapdeal.com') and hard_exception=1 then 1 
     when updated_by in ('kshitij.srivastava@snapdeal.com','ashwani.kumar03@snapdeal.com') and hard_exception=0 then 0
     when updated_by not in ('kshitij.srivastava@snapdeal.com','ashwani.kumar03@snapdeal.com') and hard_exception=1 then 2 
     when updated_by not in ('kshitij.srivastava@snapdeal.com','ashwani.kumar03@snapdeal.com') and hard_exception=0 then 0 
     else hard_exception end as*/  hard_exception,
updated_date,updated_by,
row_number()over(partition by vendorcode order by updated_date desc,is_exception desc,hard_exception desc) as ranks
from analytics_pricing.vendorcode_exception_list rf
)a
where ranks=1;


DROP TABLE IF EXISTS brand_vendorcode_exception_list;
CREATE LOCAL TEMP TABLE brand_vendorcode_exception_list on commit preserve rows as
select *
from
(
select 
brand_id,
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
is_exception,hard_exception,updated_Date,
row_number()over(partition by brand_id,vendor_code order by updated_date desc,is_exception desc,hard_exception desc) as ranks
from analytics_pricing.brand_vendorcode_exception_list rf
)a
where ranks=1;


DROP TABLE IF EXISTS brand_mandate_productss;
CREATE LOCAL TEMP TABLE brand_mandate_productss on commit preserve rows as
select *
from
(
select 
case when substring(supc, 1, 1)='sdl' then initcap(supc) else supc end as supc,
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
cashback_enabled,updated_date,
row_number()over(partition by supc,vendor_code order by updated_date desc,cashback_enabled desc) as ranks
from analytics_pricing.brand_mandate_product rf
)a
where ranks=1
union 
select *
from
( 
select 
case when substring(supc, 1, 1)='sdl' then initcap(supc) else supc end as supc,
case when substring(a.vendor_code, 1, 1)='s' then initcap(a.vendor_code) else a.vendor_code end as vendor_code,
enabled as cashback_enabled,updated_date,
row_number()over(partition by supc,a.vendor_code order by a.updated_date desc,enabled desc) as ranks 
from analytics_pricing.brand_mandate_vendor_brand a
join analytics_pricing.cash_back_final_update_supc_vendorcode b on lower(a.vendor_code)=lower(b.vendor_code) and a.brand_id=b.brand_id
)a
where ranks=1
;

DROP TABLE IF EXISTS category_team_request;
CREATE LOCAL TEMP TABLE category_team_request on commit preserve rows as
select *
from
(
select 
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
cashback_enabled,updated_Date,
row_number()over(partition by vendor_code order by updated_date desc,cashback_enabled desc) as ranks
from analytics_pricing.category_team_request rf
)a
where ranks=1
union 
SELECT distinct case when substring(code, 1, 1)='s' then initcap(code) else code end as vendor_code,
1 as cashback_enabled,created,1 as rank
FROM dwh.d_product_vendor WHERE DATE(created)>=CURRENT_DATE-60;

DROP TABLE IF EXISTS price_drop_vendor;
CREATE LOCAL TEMP TABLE price_drop_vendor on commit preserve rows as
select *
from
(
select 
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
cashback_enabled,updated_Date,
row_number()over(partition by vendor_code order by updated_date desc,cashback_enabled desc) as ranks
from analytics_pricing.price_drop_vendor rf
)a
where ranks=1;

DROP TABLE IF EXISTS sale_participation_vendor_list;
CREATE LOCAL TEMP TABLE sale_participation_vendor_list on commit preserve rows as
select distinct 
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
'Yes' as Participation
from analytics_logistics.CPT_sale_seller_list 
where end_date in (select max(end_Date) from analytics_logistics.CPT_sale_seller_list);

/*select 
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
case when substring(Participation, 1, 1)='n' then initcap(Participation)
     when substring(Participation, 1, 1)='y' then initcap(Participation) 
     when substring(Participation, 1, 1)='o' then initcap(Participation)
else Participation end as Participation
from analytics_pricing.sale_participation_vendor_list ;*/

DROP TABLE IF EXISTS bad_vendor_code_list;
CREATE LOCAL TEMP TABLE bad_vendor_code_list on commit preserve rows as
select *
from (
select distinct 
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
is_exception,hard_exception,updated_Date,
row_number()over(partition by vendor_code order by updated_date desc,is_exception desc) as ranks
from analytics_pricing.bad_vendor_code_list
)a
;

DROP TABLE IF EXISTS CPT_sale_seller_list;
CREATE LOCAL TEMP TABLE CPT_sale_seller_list on commit preserve rows as
select distinct case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
start_Date,end_Date,bid,updated_date,created_date
from analytics_logistics.CPT_sale_seller_list
where end_date in (select max(end_Date) from analytics_logistics.CPT_sale_seller_list);

Truncate table analytics_pricing.cpt_active_seller_supc ;
Insert into analytics_pricing.cpt_active_seller_supc
select 
        * 
from
        (
        select 
                *,     
                row_number() over(partition by activeDay,sellerCode,supc order by bid asc) as ord
        from 
        analytics_pricing.active_cpt_seller_supc
        ) active 
where 
        ord = 1 and activeDay=current_date-1
;

drop table if exists motz_cb ;
CREATE LOCAL TEMP TABLE motz_cb on commit preserve rows as
select *
from 
(select *,rank() over (partition by  lower(supc), lower(vendor_Code) order by start_datetime DESC) AS rank1 
from 
(
SELECT 
case when substring(supc, 1, 1)='sdl' then initcap(supc) else supc end as supc,
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
RO_Number, cash_back, start_datetime, end_Datetime,SD_Share,
case when substring(include_algo_cb, 1, 1)='n' then initcap(include_algo_cb) else include_algo_cb end as cat_motz_CB_add,
flag
from analytics_pricing.monetization_supc  
where Flag='Non-SLI' and current_date between date(start_datetime) and date(end_datetime)
GROUP BY 1,2,3,4,5,6,7,8,9
)a
)a
where rank1=1 ;

drop table if exists pl_products_bau_cb ;
CREATE LOCAL TEMP TABLE pl_products_bau_cb on commit preserve rows as
select
distinct  
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
case when substring(supc, 1, 1)='sdl' then initcap(supc) else supc end as supc,
period,remarks,brand_name,selling_price,Current_CBPU,Desired_OP_SALE,New_CBPU,created,updated,BU_Head
from analytics_pricing.pl_products_sale_bau_cb 
where period='BAU'
;

drop table if exists pl_products_sale_cb ;
CREATE LOCAL TEMP TABLE pl_products_sale_cb on commit preserve rows as
select
distinct  
case when substring(vendor_code, 1, 1)='s' then initcap(vendor_code) else vendor_code end as vendor_code,
case when substring(supc, 1, 1)='sdl' then initcap(supc) else supc end as supc,
period,remarks,brand_name,selling_price,Current_CBPU,Desired_OP_SALE,New_CBPU,created,updated,BU_Head
from analytics_pricing.pl_products_sale_bau_cb 
where period='Sale'
;

truncate table analytics_pricing.product_disablement_snapshot_dump ;
insert into analytics_pricing.product_disablement_snapshot_dump 
select 
case when substring(seller_code, 1, 1)='s' then initcap(seller_code) else seller_code end as seller_code,
upper(supc) as supc,
product_sid,vendor_sid,
new_logic_status,new_logic_reason,seller_zone 
from 
analytics_logistics.product_disablement_snapshot 
group by 1,2,3,4,5,6,7
;

UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step3';


INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step4',(SELECT NOW()),NULL;

select refresh('analytics_pricing.base_tables1');

--select refresh('analytics_pricing.subcat_min_gm_cutoff');

SELECT ANALYZE_STATISTICS('analytics_pricing.base_tables1');

select refresh('analytics_pricing.product_disablement_snapshot_dump');
SELECT ANALYZE_STATISTICS('analytics_pricing.product_disablement_snapshot_dump');

TRUNCATE TABLE analytics_pricing.base_tables2_exception_flag;
insert into analytics_pricing.base_tables2_exception_flag 
select distinct a.*,
ifnull(bm.cashback_enabled,0) as 'brand_mandate_exception',
ifnull(sub.hard_exception,0) as 'subcategory_exception',
ifnull(sv.hard_exception,0) as 'subcat_vendor_exception',
ifnull(br.hard_exception,0) as 'brand_exception',
ifnull(el.hard_exception,0) as 'supc_vendor_exception',
ifnull(sel.hard_exception,0) as 'supc_exception',
ifnull(vel.hard_exception,0) as 'vendor_exception',
ifnull(bvl.hard_exception,0) as 'brand_vendor_exception',
case when ifnull(sub.is_exception,0)=1 or ifnull(sv.is_exception,0)=1 or ifnull(br.is_exception,0) =1
or ifnull(el.is_exception,0)=1 or ifnull(sel.is_exception,0)=1 or ifnull(vel.is_exception,0)=1
or ifnull(bvl.is_exception,0)=1 then 1 else 0 end as 'is_exception',
case when ifnull(sub.hard_exception,0)=1 or ifnull(sv.hard_exception,0)=1 or ifnull(br.hard_exception,0) =1
or ifnull(el.hard_exception,0)=1 or ifnull(sel.hard_exception,0)=1 or ifnull(vel.hard_exception,0)=1 or ifnull(vel.hard_exception,0)=2
or ifnull(bvl.hard_exception,0)=1 then 1 else 0 end as 'is_hard_exception',
ifnull(bv.is_exception,0) as 'bad_seller',
case when hcb.pog_id is not null then 1 else 0 end as 'high_cb_pog',
case when sp.vendor_code is not null then Participation else 'Other' end as 'sale_participation',
case when v.vendor_sid is not null then 1 else 0 end as 'top_vendor',
ifnull(ct.cashback_enabled,0) as 'category_request_cb',
ifnull(pd.cashback_enabled,0) as 'price_drop_vendor',
sb.variant1_name,sb.variant1_min_gm_percent,sb.variant1_gm_floor,sb.variant1_nm_percent,sb.variant1_adjusted,	
sb.variant2_name,sb.variant2_min_gm_percent,sb.variant2_gm_floor,sb.variant2_nm_percent,sb.variant2_adjusted,	
variant3_name,variant3_min_gm_percent,variant3_gm_floor,variant3_nm_percent,variant3_adjusted,	
variant4_name,variant4_min_gm_percent,variant4_gm_floor,variant4_nm_percent,variant4_adjusted,	
variant5_name,variant5_min_gm_percent,variant5_gm_floor,variant5_nm_percent,variant5_adjusted,
variant6_name,variant6_min_gm_percent,variant6_gm_floor,variant6_nm_percent,variant6_adjusted,
spcm.variant1_name as variant7_name,spcm.variant1_min_gm_percent as variant7_min_gm_percent,spcm.variant1_gm_floor as variant7_gm_floor,spcm.variant1_nm_percent as variant7_nm_percent,spcm.variant1_adjusted as variant7_adjusted,	
spcm.variant2_name as variant8_name,spcm.variant2_min_gm_percent as variant8_min_gm_percent,spcm.variant2_gm_floor as variant8_gm_floor,spcm.variant2_nm_percent as variant8_nm_percent,spcm.variant2_adjusted as variant8_adjusted,	
spcm.variant1_min_gm_percent as price_drop_pct,reduce_sp_pct,pct as cm_gm_movement_pct,
cpt.bid,
ifnull((a.selling_price*ifnull(cpt1.bid,0)/100000000)*0.82*ifnull(1-rto_factor,0)/(1-ifnull(rto_factor,0)-ifnull(rpr_factor,0)),0) as bid_active_cpt,
rto_factor,
rpr_factor,
ifnull((a.selling_price*ifnull(cpt.bid,0)/100000000),0) as cpt_income,
ifnull(((ifnull(reverse_logistics_charges_rto_forward,0)*ifnull(rto_factor,0))/(1-ifnull(rto_factor,0)-ifnull(rpr_factor,0)))/1.18,0) as rto_recovery,
ifnull((((ifnull(reverse_logistics_charges_rpr_forward,0)+ifnull(payment_collection_charges,0)+ifnull(oneship_charges,0))*ifnull(rpr_factor,0))/(1-ifnull(rto_factor,0)-ifnull(rpr_factor,0)))/1.18,0) as rpr_recovery,
ifnull(case when temp1.ro_type='GSV' then ifnull(temp1.Amount,0)*0.01*
                                         (case when temp1.rule='DEL' then ifnull(a.selling_price,0)
                                               when temp1.rule='Ship' then ifnull(a.selling_price,0)/(1-ifnull(rto_factor,0)-ifnull(rpr_factor,0))
                                               else 0 end)
           when temp1.ro_type='LSM' then  ifnull(ratio,0)*
                                      (case when temp1.rule='DEL' then ifnull(a.selling_price,0)
                                               when temp1.rule='Ship' then ifnull(a.selling_price,0)/(1-ifnull(rto_factor,0)-ifnull(rpr_factor,0))
                                               else 0 end)   
                  else 0 end,0) as ro_monetisation,
                                  
ifnull(cash_back,0) as cat_motz_cb, 
ifnull(cat_motz_CB_add,'No') as cat_motz_CB_add,
ifnull(plb.Desired_OP_SALE,0) as pl_bau_op,
ifnull(plb.New_CBPU,0) as pl_bau_cb,
ifnull(pls.Desired_OP_SALE,0) as pl_sale_op,
ifnull(pls.New_CBPU,0) as pl_sale_cb,
CategoryGroupNew,
IsCore_new,
Final_ElasticityTag,
Elasticity_Pct,
new_logic_status as new_logic_status,
new_logic_reason as new_logic_reason,
seller_zone as seller_zone,
ifnull(sop.selling_price,99) as bau_op,
ifnull(sop.selling_price2,1) as sale_op
             
from analytics_pricing.base_tables1 a
left join vendor1 v on v.vendor_sid=a.vendor_sid
left join subcategoryId_exception_list sub on sub.subcategory_id=a.subcategory_id
left join subcategoryId_vendorCode_exception_list sv on sv.subcategory_id=a.subcategory_id and sv.vendor_code=a.vendor_Code
left join brandId_brandname_exception_list br on br.brand_id=a.brand_id
left join supc_vendorcode_exception_list el on el.supc=a.supc and el.vendorCode=a.vendor_code
left join supc_exception_list sel on sel.supc=a.supc
left join vendorcode_exception_list vel on vel.vendorCode=a.vendor_code
left join brand_vendorcode_exception_list bvl on a.brand_id=bvl.brand_id and a.vendor_code=bvl.vendor_code
left join brand_mandate_productss bm on bm.supc=a.supc and bm.vendor_code=a.vendor_code
left join bad_vendor_code_list bv on bv.vendor_code=a.vendor_code
left join sale_participation_vendor_list sp on sp.vendor_code=a.vendor_code
left join category_team_request ct on ct.vendor_Code=a.vendor_code
left join analytics_pricing.high_cb_pog_id hcb on hcb.pog_id=a.pog_id
left join price_drop_vendor pd on pd.vendor_code=a.vendor_code 
left join analytics_pricing.subcat_min_gm_cutoff_old_logic spcm on spcm.subcategory_id=a.subcategory_id and a.selling_price between spcm.from_price and spcm.to_price
left join analytics_pricing.subcat_min_gm_cutoff sb on sb.subcategory_id=a.subcategory_id and a.selling_price between sb.from_price and sb.to_price
left join analytics_pricing.vendor_list_reduce_sp vl on vl.vendor_code=a.vendor_code
left join CPT_sale_seller_list cpt on lower(cpt.vendor_code)=lower(a.vendor_code) 
left join analytics_pricing.cm_gm_movement_vendor_list cm on cm.subcategory_id=a.subcategory_id and cm.vendor_code=a.vendor_code
left join analytics_logistics.return_factor_final rf on rf.subcategory_id=a.subcategory_id
left join analytics_pricing.cpt_active_seller_supc cpt1 on lower(cpt1.supc)=lower(a.supc) and lower(cpt1.sellerCode)=lower(a.vendor_code)
left join analytics_logistics.temp_base_3 temp1 on temp1.brand_id=a.brand_id and lower(temp1.seller_code)=lower(a.vendor_code) and current_date-1 between ro_start_date and ro_end_date
left join motz_cb mcb on mcb.supc=a.supc and mcb.vendor_code=a.vendor_code
left join pl_products_bau_cb plb on plb.supc=a.supc and plb.vendor_code=a.vendor_code 
left join pl_products_sale_cb pls on pls.supc=a.supc and pls.vendor_code=a.vendor_code 
left join analytics_pricing.categorygroupmapping cg on a.subcategory_id=cg.subcategory_id
left join analytics_pricing.subcat_op_cutoff_test sop on a.subcategory_id=sop.subcategory_id and a.bucket_id=sop.bucket_id
left join analytics_pricing.product_disablement_snapshot_dump  pds on pds.product_sid=a.product_sid and pds.vendor_sid=a.vendor_sid
where seller_price>0 
--and a.supc='1345363'
;


/*update analytics_pricing.base_tables2_exception_flag a
set new_logic_status=pds.new_logic_status,
    new_logic_reason=pds.new_logic_reason,
    seller_zone=pds.seller_zone
from analytics_pricing.product_disablement_snapshot_dump pds 
where pds.supc=a.supc and pds.seller_code=a.vendor_code
;
*/

UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step4';


INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step5',(SELECT NOW()),NULL;

TRUNCATE TABLE analytics_pricing.base_tables3_cb;
insert into analytics_pricing.base_tables3_cb 
select distinct a.*,
                case when ((a.is_enabled_with_inv=1) or ((new_supercategory not in ('Refurbished','Books','TV Shop','Motors','Real Estates','Snapdeal Select','NA') 
                      and a.subcategory_id not in ('676')) and a.vip_updated>=current_date-360 )) then 1 else 0 end as 'live_flag',
                      
Is_pushed,Sale_Variant,Non_Sale_Variant,                      

/*case when sd_share_pre_cashback<=LEAST(base_variant_floor+(Slab-1)*30,base_variant_floor+5*30)  then 0
     when sd_share_pre_cashback - (sd_share_pre_cashback*base_variant_cb/(case when base_variant_adjusted=0 then 1 else 1.18 end)) <= LEAST(base_variant_floor+(Slab-1)*30,base_variant_floor+5*30) 
     then greatest((sd_share_pre_cashback-LEAST(base_variant_floor+(Slab-1)*30,base_variant_floor+5*30))*(case when base_variant_adjusted=0 then 1 else 1.18 end)-(selling_price*ifnull(price_drop_pct,0)),0)			
     else greatest(sd_share_pre_cashback*base_variant_cb-(selling_price*greatest(ifnull(price_drop_pct,0),0)),0) end
     
case 
     when LEAST((sd_share_pre_cashback-ifnull(variant1_min_gm_percent+ifnull(reduce_sp_pct,0)+ifnull(cm_gm_movement_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30)-variant1_nm_percent*selling_price))*(case when variant1_adjusted=0 then 1 else 1.18 end)<=0 then 0
     else LEAST((sd_share_pre_cashback-ifnull(variant1_min_gm_percent+ifnull(reduce_sp_pct,0)+ifnull(cm_gm_movement_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30)-variant1_nm_percent*selling_price))*(case when variant1_adjusted=0 then 1 else 1.18 end) 
     end as 'base_cb'
*/ 

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant1_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30)-variant1_nm_percent*selling_price))*(case when variant1_adjusted=0 then 1 else 1.18 end)<=0 then 0
     else LEAST((sd_share_pre_cashback-ifnull(variant1_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30)-variant1_nm_percent*selling_price))*(case when variant1_adjusted=0 then 1 else 1.18 end) 
     end as 'base_cb',

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant2_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30)-variant2_nm_percent*selling_price))*(case when variant2_adjusted=0 then 1 else 1.18 end)<=0 then 0
     else LEAST((sd_share_pre_cashback-ifnull(variant2_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30)-variant2_nm_percent*selling_price))*(case when variant2_adjusted=0 then 1 else 1.18 end) 
     end as 'base_cb1',

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant7_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant7_gm_floor+(Slab-1)*30,variant7_gm_floor+9*30)-variant7_nm_percent*selling_price))*(case when variant7_adjusted=0 then 1 else 1.18 end)<=0 then 0
     else LEAST((sd_share_pre_cashback-ifnull(variant7_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant7_gm_floor+(Slab-1)*30,variant7_gm_floor+9*30)-variant7_nm_percent*selling_price))*(case when variant7_adjusted=0 then 1 else 1.18 end) 
     end as 'base_cb2',

/*case 
     when (sd_share_pre_cashback-LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30)-ifnull(variant3_nm_percent+ifnull(reduce_sp_pct,0)+ifnull(cm_gm_movement_pct,0),0.8)*selling_price)*(case when variant3_adjusted=0 then 1 else 1.18 end)<=0 then 0
     else (sd_share_pre_cashback-LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30)-ifnull(variant3_nm_percent+ifnull(reduce_sp_pct,0)+ifnull(cm_gm_movement_pct,0),0.8)*selling_price)*(case when variant3_adjusted=0 then 1 else 1.18 end)
     end as 'base_cb2',-- GM_to_NM_formula*/     

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant8_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant8_gm_floor+(Slab-1)*30,variant8_gm_floor+9*30)-variant8_nm_percent*selling_price))*(case when variant8_adjusted=0 then 1 else 1.18 end)<=0 then 0
     else LEAST((sd_share_pre_cashback-ifnull(variant8_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant8_gm_floor+(Slab-1)*30,variant8_gm_floor+9*30)-variant8_nm_percent*selling_price))*(case when variant8_adjusted=0 then 1 else 1.18 end) 
     end as 'base_cb3',	

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant2_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30)-variant2_nm_percent*selling_price))*(case when variant2_adjusted=0 then 1 else 1.18 end)<=0 then 0
     else LEAST((sd_share_pre_cashback-ifnull(variant2_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30)-variant2_nm_percent*selling_price))*(case when variant2_adjusted=0 then 1 else 1.18 end) 
     end as 'base_cb4',	     

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant2_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30)-variant2_nm_percent*selling_price))*(case when variant2_adjusted=0 then 1 else 1.18 end)<=0 then 0
     else LEAST((sd_share_pre_cashback-ifnull(variant2_min_gm_percent+ifnull(reduce_sp_pct,0),sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30)-variant2_nm_percent*selling_price))*(case when variant2_adjusted=0 then 1 else 1.18 end) 
     end as 'base_cb5',

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant3_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30)-variant3_nm_percent*selling_price))*(case when variant3_adjusted=0 then 1 else 1.18 end)<=0 then 0 +(case when variant3_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when vendor_code in ('Sb0dff','S9d203','Sba5d6') then selling_price*0.11 /*Sukhi Vendor Motz*/ else 0 end)
     else LEAST((sd_share_pre_cashback-ifnull(variant3_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30)-variant3_nm_percent*selling_price))*(case when variant3_adjusted=0 then 1 else 1.18 end)           +(case when variant3_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when vendor_code in ('Sb0dff','S9d203','Sba5d6') then selling_price*0.11 /*Sukhi Vendor Motz*/ else 0 end)
     end as 'sale1_cb',

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant4_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30)-variant4_nm_percent*selling_price))*(case when variant4_adjusted=0 then 1 else 1.18 end)<=0 then 0 +(case when variant4_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when vendor_code in ('Sb0dff','S9d203','Sba5d6') then selling_price*0.11 /*Sukhi Vendor Motz*/ else 0 end)
     else LEAST((sd_share_pre_cashback-ifnull(variant4_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30)-variant4_nm_percent*selling_price))*(case when variant4_adjusted=0 then 1 else 1.18 end)           +(case when variant4_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when vendor_code in ('Sb0dff','S9d203','Sba5d6') then selling_price*0.11 /*Sukhi Vendor Motz*/ else 0 end)
     end as 'sale2_cb',
                  
/*
CBPU 130 logic
case 
     when LEAST((sd_share_pre_cashback-ifnull(variant3_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30)-variant3_nm_percent*selling_price))*(case when variant3_adjusted=0 then 1 else 1.18 end)<=0 then 0 +(case when variant3_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when IsCore_new='Yes' and selling_price>300 and variant3_name<>'BAU_0NM_base_58' and subcategory_id not in ('252', '34', '540', '248', '564', '223', '1172', '110', '525', '239', '179', '233', '419', '600') then bid_active_cpt+cpt_income else 0 end)
     else LEAST((sd_share_pre_cashback-ifnull(variant3_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30)-variant3_nm_percent*selling_price))*(case when variant3_adjusted=0 then 1 else 1.18 end)           +(case when variant3_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when IsCore_new='Yes' and selling_price>300 and variant3_name<>'BAU_0NM_base_58' and subcategory_id not in ('252', '34', '540', '248', '564', '223', '1172', '110', '525', '239', '179', '233', '419', '600') then bid_active_cpt+cpt_income else 0 end)
     end as 'sale1_cb',

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant4_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30)-variant4_nm_percent*selling_price))*(case when variant4_adjusted=0 then 1 else 1.18 end)<=0 then 0 +(case when variant4_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when IsCore_new='Yes' and selling_price>300 and variant4_name<>'BAU_0NM_base_58' and subcategory_id not in ('252', '34', '540', '248', '564', '223', '1172', '110', '525', '239', '179', '233', '419', '600') then bid_active_cpt+cpt_income else 0 end)
     else LEAST((sd_share_pre_cashback-ifnull(variant4_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30)-variant4_nm_percent*selling_price))*(case when variant4_adjusted=0 then 1 else 1.18 end)           +(case when variant4_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when IsCore_new='Yes' and selling_price>300 and variant4_name<>'BAU_0NM_base_58' and subcategory_id not in ('252', '34', '540', '248', '564', '223', '1172', '110', '525', '239', '179', '233', '419', '600') then bid_active_cpt+cpt_income else 0 end)
     end as 'sale2_cb',

case when sd_share_pre_cashback<=LEAST(sale1_variant_floor+(Slab-1)*30,sale1_variant_floor+9*30)  then 0
     when sd_share_pre_cashback - (sd_share_pre_cashback*sale1_variant_cb/(case when sale1_variant_adjusted=0 then 1 else 1.18 end)) <= LEAST(sale1_variant_floor+(Slab-1)*30,sale1_variant_floor+9*30) 
     then (sd_share_pre_cashback-LEAST(sale1_variant_floor+(Slab-1)*30,sale1_variant_floor+9*30))*(case when sale1_variant_adjusted=0 then 1 else 1.18 end)			
     else sd_share_pre_cashback*sale1_variant_cb end  as 'sale1_cb',

case when sd_share_pre_cashback<=LEAST(sale2_variant_floor+(Slab-1)*30,sale2_variant_floor+9*30)  then 0
     when sd_share_pre_cashback - (sd_share_pre_cashback*sale2_variant_cb/(case when sale2_variant_adjusted=0 then 1 else 1.18 end)) <= LEAST(sale2_variant_floor+(Slab-1)*30,sale2_variant_floor+9*30) 
     then (sd_share_pre_cashback-LEAST(sale2_variant_floor+(Slab-1)*30,sale2_variant_floor+9*30))*(case when sale2_variant_adjusted=0 then 1 else 1.18 end)			
     else sd_share_pre_cashback*sale2_variant_cb end  as 'sale2_cb',

case when sd_share_pre_cashback<=LEAST(sale3_variant_floor+(Slab-1)*30,sale3_variant_floor+9*30)  then 0
     when sd_share_pre_cashback - (sd_share_pre_cashback*sale3_variant_cb/(case when sale3_variant_adjusted=0 then 1 else 1.18 end)) <= LEAST(sale3_variant_floor+(Slab-1)*30,sale3_variant_floor+9*30) 
     then (sd_share_pre_cashback-LEAST(sale3_variant_floor+(Slab-1)*30,sale3_variant_floor+9*30))*(case when sale3_variant_adjusted=0 then 1 else 1.18 end)			
     else sd_share_pre_cashback*sale3_variant_cb end as old*/

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant5_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant5_gm_floor+(Slab-1)*30,variant5_gm_floor+9*30)-variant5_nm_percent*selling_price))*(case when variant5_adjusted=0 then 1 else 1.18 end)<=0 then 0 +(case when variant5_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when vendor_code in ('Sb0dff','S9d203','Sba5d6') then selling_price*0.11 /*Sukhi Vendor Motz*/ else 0 end)
     else LEAST((sd_share_pre_cashback-ifnull(variant5_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant5_gm_floor+(Slab-1)*30,variant5_gm_floor+9*30)-variant5_nm_percent*selling_price))*(case when variant5_adjusted=0 then 1 else 1.18 end) 			+(case when variant5_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when vendor_code in ('Sb0dff','S9d203','Sba5d6') then selling_price*0.11 /*Sukhi Vendor Motz*/ else 0 end)
     end as 'sale3_cb',

case 
     when LEAST((sd_share_pre_cashback-ifnull(variant6_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant6_gm_floor+(Slab-1)*30,variant6_gm_floor+9*30)-variant6_nm_percent*selling_price))*(case when variant6_adjusted=0 then 1 else 1.18 end)<=0 then 0 +(case when variant6_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when vendor_code in ('Sb0dff','S9d203','Sba5d6') then selling_price*0.11 /*Sukhi Vendor Motz*/ else 0 end)
     else LEAST((sd_share_pre_cashback-ifnull(variant6_min_gm_percent,sd_share_pre_cashback/selling_price)*selling_price),(sd_share_pre_cashback-LEAST(variant6_gm_floor+(Slab-1)*30,variant6_gm_floor+9*30)-variant6_nm_percent*selling_price))*(case when variant6_adjusted=0 then 1 else 1.18 end) 			+(case when variant6_name='BAU_0NM_base_58' then (selling_price*ifnull(bid,0)/100000000) else 0 end)+(case when vendor_code in ('Sb0dff','S9d203','Sba5d6') then selling_price*0.11 /*Sukhi Vendor Motz*/ else 0 end)
     end as 'sale4_cb'
  
from analytics_pricing.base_tables2_exception_flag a  

left join analytics_pricing.pushed_status b on a.is_enabled=b.is_enabled and a.is_enabled_with_inv=b.is_enabled_with_inv and a.Sold_30_flag_pog=b.Sold_30_flag_pog
and a.Sold_7_flag_pog=b.Sold_7_flag_pog and a.top_vendor=b.top_vendor and a.category_request_cb=b.Category_CB_Push
left join analytics_pricing.cashback_update_exception_mapping c on (a.is_exception=c.is_exception and a.brand_mandate_exception=c.brand_mandate_exception 
and a.is_hard_exception=c.is_hard_exception and a.bad_seller=c.bad_seller and a.sale_participation=c.sale_participation and a.price_drop_vendor=c.price_drop_vendor 
and a.high_cb_pog=c.high_cb_pog)
/*where supc='SDL014712573'
*/;


UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step5';


INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step6',(SELECT NOW()),NULL;



TRUNCATE TABLE analytics_pricing.Cashback_list;
insert into analytics_pricing.Cashback_list 
select m.*, 

/*
case when (vip_selling_price-base_cb)<79 or (vip_selling_price-base_cb)>=1999 then  base_cb
     when (vip_selling_price-base_cb)= a.rounding then base_cb
	 when (vip_selling_price-base_cb)= a.rounding_next then base_cb
	 
     when (vip_selling_price-base_cb)< a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding) and (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))
	 when (vip_selling_price-base_cb)< a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding))/(case when base_variant_adjusted=0 then 1 else 1.18 end)>=LEAST(base_variant_floor+(Slab-1)*30,base_variant_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding))
	 when (vip_selling_price-base_cb)< a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding) and (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))
          
     when (vip_selling_price-base_cb) between a.rounding and a.rounding_1 and (a.rounding_1-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding) and (base_cb-(a.rounding_1-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_1-(vip_selling_price-base_cb)))
	 when (vip_selling_price-base_cb) between a.rounding and a.rounding_1 and (a.rounding_1-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding))/(case when base_variant_adjusted=0 then 1 else 1.18 end)>=LEAST(base_variant_floor+(Slab-1)*30,base_variant_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding))
     when (vip_selling_price-base_cb) between a.rounding and a.rounding_1 and (a.rounding_1-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding) and (base_cb-(a.rounding_1-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_1-(vip_selling_price-base_cb)))
     
	 when (vip_selling_price-base_cb) between a.rounding_1 and a.rounding_2 and (a.rounding_2-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding_1) and (base_cb-(a.rounding_2-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_2-(vip_selling_price-base_cb)))
	 when (vip_selling_price-base_cb) between a.rounding_1 and a.rounding_2 and (a.rounding_2-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_1) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding_1))/(case when base_variant_adjusted=0 then 1 else 1.18 end)>=LEAST(base_variant_floor+(Slab-1)*30,base_variant_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding_1))
     when (vip_selling_price-base_cb) between a.rounding_1 and a.rounding_2 and (a.rounding_2-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_1) and (base_cb-(a.rounding_2-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_2-(vip_selling_price-base_cb)))
     
	 when (vip_selling_price-base_cb) between a.rounding_2 and a.rounding_3 and (a.rounding_3-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding_2) and (base_cb-(a.rounding_3-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_3-(vip_selling_price-base_cb)))
     when (vip_selling_price-base_cb) between a.rounding_2 and a.rounding_3 and (a.rounding_3-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_2) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding_2))/(case when base_variant_adjusted=0 then 1 else 1.18 end)>=LEAST(base_variant_floor+(Slab-1)*30,base_variant_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding_2))
     when (vip_selling_price-base_cb) between a.rounding_2 and a.rounding_3 and (a.rounding_3-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_2) and (base_cb-(a.rounding_3-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_3-(vip_selling_price-base_cb)))
     
	 when (vip_selling_price-base_cb) between a.rounding_3 and a.rounding_4 and (a.rounding_4-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding_3) and (base_cb-(a.rounding_4-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_4-(vip_selling_price-base_cb)))
     when (vip_selling_price-base_cb) between a.rounding_3 and a.rounding_4 and (a.rounding_4-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_3) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding_3))/(case when base_variant_adjusted=0 then 1 else 1.18 end)>=LEAST(base_variant_floor+(Slab-1)*30,base_variant_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding_3))
     when (vip_selling_price-base_cb) between a.rounding_3 and a.rounding_4 and (a.rounding_4-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_3) and (base_cb-(a.rounding_4-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_4-(vip_selling_price-base_cb)))
	 
	 when (vip_selling_price-base_cb) between a.rounding_4 and a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding_4) and (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))
     when (vip_selling_price-base_cb) between a.rounding_4 and a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_4) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding_4))/(case when base_variant_adjusted=0 then 1 else 1.18 end)>=LEAST(base_variant_floor+(Slab-1)*30,base_variant_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding_4))
     when (vip_selling_price-base_cb) between a.rounding_4 and a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_4) and (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))

     else (base_cb) end as base_cb_rounded,
*/

case when (vip_selling_price-base_cb)<79 or (vip_selling_price-base_cb)>=1999 then  base_cb
     when (vip_selling_price-base_cb)= a.rounding then base_cb
	 when (vip_selling_price-base_cb)= a.rounding_next then base_cb
	 
     when (vip_selling_price-base_cb)< a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding) and (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))
	 when (vip_selling_price-base_cb)< a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding))
	 when (vip_selling_price-base_cb)< a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding) and (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))
          
     when (vip_selling_price-base_cb) between a.rounding and a.rounding_1 and (a.rounding_1-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding) and (base_cb-(a.rounding_1-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_1-(vip_selling_price-base_cb)))
	 when (vip_selling_price-base_cb) between a.rounding and a.rounding_1 and (a.rounding_1-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding))
     when (vip_selling_price-base_cb) between a.rounding and a.rounding_1 and (a.rounding_1-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding) and (base_cb-(a.rounding_1-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_1-(vip_selling_price-base_cb)))
     
	 when (vip_selling_price-base_cb) between a.rounding_1 and a.rounding_2 and (a.rounding_2-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding_1) and (base_cb-(a.rounding_2-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_2-(vip_selling_price-base_cb)))
	 when (vip_selling_price-base_cb) between a.rounding_1 and a.rounding_2 and (a.rounding_2-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_1) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding_1))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding_1))
     when (vip_selling_price-base_cb) between a.rounding_1 and a.rounding_2 and (a.rounding_2-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_1) and (base_cb-(a.rounding_2-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_2-(vip_selling_price-base_cb)))
     
	 when (vip_selling_price-base_cb) between a.rounding_2 and a.rounding_3 and (a.rounding_3-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding_2) and (base_cb-(a.rounding_3-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_3-(vip_selling_price-base_cb)))
     when (vip_selling_price-base_cb) between a.rounding_2 and a.rounding_3 and (a.rounding_3-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_2) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding_2))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding_2))
     when (vip_selling_price-base_cb) between a.rounding_2 and a.rounding_3 and (a.rounding_3-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_2) and (base_cb-(a.rounding_3-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_3-(vip_selling_price-base_cb)))
     
	 when (vip_selling_price-base_cb) between a.rounding_3 and a.rounding_4 and (a.rounding_4-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding_3) and (base_cb-(a.rounding_4-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_4-(vip_selling_price-base_cb)))
     when (vip_selling_price-base_cb) between a.rounding_3 and a.rounding_4 and (a.rounding_4-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_3) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding_3))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding_3))
     when (vip_selling_price-base_cb) between a.rounding_3 and a.rounding_4 and (a.rounding_4-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_3) and (base_cb-(a.rounding_4-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_4-(vip_selling_price-base_cb)))
	 
	 when (vip_selling_price-base_cb) between a.rounding_4 and a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))<= ((vip_selling_price-base_cb)-a.rounding_4) and (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))
     when (vip_selling_price-base_cb) between a.rounding_4 and a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_4) and sd_share_pre_cashback-(base_cb+((vip_selling_price-base_cb)-a.rounding_4))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (base_cb+((vip_selling_price-base_cb)-a.rounding_4))
     when (vip_selling_price-base_cb) between a.rounding_4 and a.rounding_next and (a.rounding_next-(vip_selling_price-base_cb))>= ((vip_selling_price-base_cb)-a.rounding_4) and (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))>=0 then (base_cb-(a.rounding_next-(vip_selling_price-base_cb)))

     else (base_cb) end as base_cb_rounded,
	 
case when (vip_selling_price-base_cb1)<79 or (vip_selling_price-base_cb1)>=1999 then  base_cb1
     when (vip_selling_price-base_cb1)= e.rounding then base_cb1
	 when (vip_selling_price-base_cb1)= e.rounding_next then base_cb1
	 
     when (vip_selling_price-base_cb1)< e.rounding_next and (e.rounding_next-(vip_selling_price-base_cb1))<= ((vip_selling_price-base_cb1)-e.rounding) and (base_cb1-(e.rounding_next-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_next-(vip_selling_price-base_cb1)))
	 when (vip_selling_price-base_cb1)< e.rounding_next and (e.rounding_next-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding) and sd_share_pre_cashback-(base_cb1+((vip_selling_price-base_cb1)-e.rounding))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb1+((vip_selling_price-base_cb1)-e.rounding))
	 when (vip_selling_price-base_cb1)< e.rounding_next and (e.rounding_next-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding) and (base_cb1-(e.rounding_next-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_next-(vip_selling_price-base_cb1)))
          
     when (vip_selling_price-base_cb1) between e.rounding and e.rounding_1 and (e.rounding_1-(vip_selling_price-base_cb1))<= ((vip_selling_price-base_cb1)-e.rounding) and (base_cb1-(e.rounding_1-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_1-(vip_selling_price-base_cb1)))
	 when (vip_selling_price-base_cb1) between e.rounding and e.rounding_1 and (e.rounding_1-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding) and sd_share_pre_cashback-(base_cb1+((vip_selling_price-base_cb1)-e.rounding))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb1+((vip_selling_price-base_cb1)-e.rounding))
     when (vip_selling_price-base_cb1) between e.rounding and e.rounding_1 and (e.rounding_1-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding) and (base_cb1-(e.rounding_1-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_1-(vip_selling_price-base_cb1)))
     
	 when (vip_selling_price-base_cb1) between e.rounding_1 and e.rounding_2 and (e.rounding_2-(vip_selling_price-base_cb1))<= ((vip_selling_price-base_cb1)-e.rounding_1) and (base_cb1-(e.rounding_2-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_2-(vip_selling_price-base_cb1)))
	 when (vip_selling_price-base_cb1) between e.rounding_1 and e.rounding_2 and (e.rounding_2-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding_1) and sd_share_pre_cashback-(base_cb1+((vip_selling_price-base_cb1)-e.rounding_1))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb1+((vip_selling_price-base_cb1)-e.rounding_1))
     when (vip_selling_price-base_cb1) between e.rounding_1 and e.rounding_2 and (e.rounding_2-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding_1) and (base_cb1-(e.rounding_2-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_2-(vip_selling_price-base_cb1)))
     
	 when (vip_selling_price-base_cb1) between e.rounding_2 and e.rounding_3 and (e.rounding_3-(vip_selling_price-base_cb1))<= ((vip_selling_price-base_cb1)-e.rounding_2) and (base_cb1-(e.rounding_3-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_3-(vip_selling_price-base_cb1)))
     when (vip_selling_price-base_cb1) between e.rounding_2 and e.rounding_3 and (e.rounding_3-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding_2) and sd_share_pre_cashback-(base_cb1+((vip_selling_price-base_cb1)-e.rounding_2))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb1+((vip_selling_price-base_cb1)-e.rounding_2))
     when (vip_selling_price-base_cb1) between e.rounding_2 and e.rounding_3 and (e.rounding_3-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding_2) and (base_cb1-(e.rounding_3-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_3-(vip_selling_price-base_cb1)))
     
	 when (vip_selling_price-base_cb1) between e.rounding_3 and e.rounding_4 and (e.rounding_4-(vip_selling_price-base_cb1))<= ((vip_selling_price-base_cb1)-e.rounding_3) and (base_cb1-(e.rounding_4-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_4-(vip_selling_price-base_cb1)))
     when (vip_selling_price-base_cb1) between e.rounding_3 and e.rounding_4 and (e.rounding_4-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding_3) and sd_share_pre_cashback-(base_cb1+((vip_selling_price-base_cb1)-e.rounding_3))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb1+((vip_selling_price-base_cb1)-e.rounding_3))
     when (vip_selling_price-base_cb1) between e.rounding_3 and e.rounding_4 and (e.rounding_4-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding_3) and (base_cb1-(e.rounding_4-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_4-(vip_selling_price-base_cb1)))
	 
	 when (vip_selling_price-base_cb1) between e.rounding_4 and e.rounding_next and (e.rounding_next-(vip_selling_price-base_cb1))<= ((vip_selling_price-base_cb1)-e.rounding_4) and (base_cb1-(e.rounding_next-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_next-(vip_selling_price-base_cb1)))
     when (vip_selling_price-base_cb1) between e.rounding_4 and e.rounding_next and (e.rounding_next-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding_4) and sd_share_pre_cashback-(base_cb1+((vip_selling_price-base_cb1)-e.rounding_4))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb1+((vip_selling_price-base_cb1)-e.rounding_4))
     when (vip_selling_price-base_cb1) between e.rounding_4 and e.rounding_next and (e.rounding_next-(vip_selling_price-base_cb1))>= ((vip_selling_price-base_cb1)-e.rounding_4) and (base_cb1-(e.rounding_next-(vip_selling_price-base_cb1)))>=0 then (base_cb1-(e.rounding_next-(vip_selling_price-base_cb1)))

     else (base_cb1) end as base_cb1_rounded,

case when (vip_selling_price-base_cb2)<79 or (vip_selling_price-base_cb2)>=1999 then  base_cb2
     when (vip_selling_price-base_cb2)= f.rounding then base_cb2
	 when (vip_selling_price-base_cb2)= f.rounding_next then base_cb2
	 
     when (vip_selling_price-base_cb2)< f.rounding_next and (f.rounding_next-(vip_selling_price-base_cb2))<= ((vip_selling_price-base_cb2)-f.rounding) and (base_cb2-(f.rounding_next-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_next-(vip_selling_price-base_cb2)))
	 when (vip_selling_price-base_cb2)< f.rounding_next and (f.rounding_next-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding) and sd_share_pre_cashback-(base_cb2+((vip_selling_price-base_cb2)-f.rounding))/(case when variant7_adjusted=0 then 1 else 1.18 end)>=LEAST(variant7_gm_floor+(Slab-1)*30,variant7_gm_floor+9*30) then (base_cb2+((vip_selling_price-base_cb2)-f.rounding))
	 when (vip_selling_price-base_cb2)< f.rounding_next and (f.rounding_next-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding) and (base_cb2-(f.rounding_next-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_next-(vip_selling_price-base_cb2)))
          
     when (vip_selling_price-base_cb2) between f.rounding and f.rounding_1 and (f.rounding_1-(vip_selling_price-base_cb2))<= ((vip_selling_price-base_cb2)-f.rounding) and (base_cb2-(f.rounding_1-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_1-(vip_selling_price-base_cb2)))
	 when (vip_selling_price-base_cb2) between f.rounding and f.rounding_1 and (f.rounding_1-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding) and sd_share_pre_cashback-(base_cb2+((vip_selling_price-base_cb2)-f.rounding))/(case when variant7_adjusted=0 then 1 else 1.18 end)>=LEAST(variant7_gm_floor+(Slab-1)*30,variant7_gm_floor+9*30) then (base_cb2+((vip_selling_price-base_cb2)-f.rounding))
     when (vip_selling_price-base_cb2) between f.rounding and f.rounding_1 and (f.rounding_1-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding) and (base_cb2-(f.rounding_1-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_1-(vip_selling_price-base_cb2)))
     
	 when (vip_selling_price-base_cb2) between f.rounding_1 and f.rounding_2 and (f.rounding_2-(vip_selling_price-base_cb2))<= ((vip_selling_price-base_cb2)-f.rounding_1) and (base_cb2-(f.rounding_2-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_2-(vip_selling_price-base_cb2)))
	 when (vip_selling_price-base_cb2) between f.rounding_1 and f.rounding_2 and (f.rounding_2-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding_1) and sd_share_pre_cashback-(base_cb2+((vip_selling_price-base_cb2)-f.rounding_1))/(case when variant7_adjusted=0 then 1 else 1.18 end)>=LEAST(variant7_gm_floor+(Slab-1)*30,variant7_gm_floor+9*30) then (base_cb2+((vip_selling_price-base_cb2)-f.rounding_1))
     when (vip_selling_price-base_cb2) between f.rounding_1 and f.rounding_2 and (f.rounding_2-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding_1) and (base_cb2-(f.rounding_2-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_2-(vip_selling_price-base_cb2)))
     
	 when (vip_selling_price-base_cb2) between f.rounding_2 and f.rounding_3 and (f.rounding_3-(vip_selling_price-base_cb2))<= ((vip_selling_price-base_cb2)-f.rounding_2) and (base_cb2-(f.rounding_3-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_3-(vip_selling_price-base_cb2)))
     when (vip_selling_price-base_cb2) between f.rounding_2 and f.rounding_3 and (f.rounding_3-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding_2) and sd_share_pre_cashback-(base_cb2+((vip_selling_price-base_cb2)-f.rounding_2))/(case when variant7_adjusted=0 then 1 else 1.18 end)>=LEAST(variant7_gm_floor+(Slab-1)*30,variant7_gm_floor+9*30) then (base_cb2+((vip_selling_price-base_cb2)-f.rounding_2))
     when (vip_selling_price-base_cb2) between f.rounding_2 and f.rounding_3 and (f.rounding_3-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding_2) and (base_cb2-(f.rounding_3-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_3-(vip_selling_price-base_cb2)))
     
	 when (vip_selling_price-base_cb2) between f.rounding_3 and f.rounding_4 and (f.rounding_4-(vip_selling_price-base_cb2))<= ((vip_selling_price-base_cb2)-f.rounding_3) and (base_cb2-(f.rounding_4-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_4-(vip_selling_price-base_cb2)))
     when (vip_selling_price-base_cb2) between f.rounding_3 and f.rounding_4 and (f.rounding_4-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding_3) and sd_share_pre_cashback-(base_cb2+((vip_selling_price-base_cb2)-f.rounding_3))/(case when variant7_adjusted=0 then 1 else 1.18 end)>=LEAST(variant7_gm_floor+(Slab-1)*30,variant7_gm_floor+9*30) then (base_cb2+((vip_selling_price-base_cb2)-f.rounding_3))
     when (vip_selling_price-base_cb2) between f.rounding_3 and f.rounding_4 and (f.rounding_4-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding_3) and (base_cb2-(f.rounding_4-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_4-(vip_selling_price-base_cb2)))
	 
	 when (vip_selling_price-base_cb2) between f.rounding_4 and f.rounding_next and (f.rounding_next-(vip_selling_price-base_cb2))<= ((vip_selling_price-base_cb2)-f.rounding_4) and (base_cb2-(f.rounding_next-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_next-(vip_selling_price-base_cb2)))
     when (vip_selling_price-base_cb2) between f.rounding_4 and f.rounding_next and (f.rounding_next-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding_4) and sd_share_pre_cashback-(base_cb2+((vip_selling_price-base_cb2)-f.rounding_4))/(case when variant7_adjusted=0 then 1 else 1.18 end)>=LEAST(variant7_gm_floor+(Slab-1)*30,variant7_gm_floor+9*30) then (base_cb2+((vip_selling_price-base_cb2)-f.rounding_4))
     when (vip_selling_price-base_cb2) between f.rounding_4 and f.rounding_next and (f.rounding_next-(vip_selling_price-base_cb2))>= ((vip_selling_price-base_cb2)-f.rounding_4) and (base_cb2-(f.rounding_next-(vip_selling_price-base_cb2)))>=0 then (base_cb2-(f.rounding_next-(vip_selling_price-base_cb2)))

     else (base_cb2) end as base_cb2_rounded, 

case when (vip_selling_price-base_cb3)<79 or (vip_selling_price-base_cb3)>=1999 then  base_cb3
     when (vip_selling_price-base_cb3)= g.rounding then base_cb3
	 when (vip_selling_price-base_cb3)= g.rounding_next then base_cb3
	 
     when (vip_selling_price-base_cb3)< g.rounding_next and (g.rounding_next-(vip_selling_price-base_cb3))<= ((vip_selling_price-base_cb3)-g.rounding) and (base_cb3-(g.rounding_next-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_next-(vip_selling_price-base_cb3)))
	 when (vip_selling_price-base_cb3)< g.rounding_next and (g.rounding_next-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding) and sd_share_pre_cashback-(base_cb3+((vip_selling_price-base_cb3)-g.rounding))/(case when variant8_adjusted=0 then 1 else 1.18 end)>=LEAST(variant8_gm_floor+(Slab-1)*30,variant8_gm_floor+9*30) then (base_cb3+((vip_selling_price-base_cb3)-g.rounding))
	 when (vip_selling_price-base_cb3)< g.rounding_next and (g.rounding_next-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding) and (base_cb3-(g.rounding_next-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_next-(vip_selling_price-base_cb3)))
          
     when (vip_selling_price-base_cb3) between g.rounding and g.rounding_1 and (g.rounding_1-(vip_selling_price-base_cb3))<= ((vip_selling_price-base_cb3)-g.rounding) and (base_cb3-(g.rounding_1-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_1-(vip_selling_price-base_cb3)))
	 when (vip_selling_price-base_cb3) between g.rounding and g.rounding_1 and (g.rounding_1-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding) and sd_share_pre_cashback-(base_cb3+((vip_selling_price-base_cb3)-g.rounding))/(case when variant8_adjusted=0 then 1 else 1.18 end)>=LEAST(variant8_gm_floor+(Slab-1)*30,variant8_gm_floor+9*30) then (base_cb3+((vip_selling_price-base_cb3)-g.rounding))
     when (vip_selling_price-base_cb3) between g.rounding and g.rounding_1 and (g.rounding_1-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding) and (base_cb3-(g.rounding_1-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_1-(vip_selling_price-base_cb3)))
     
	 when (vip_selling_price-base_cb3) between g.rounding_1 and g.rounding_2 and (g.rounding_2-(vip_selling_price-base_cb3))<= ((vip_selling_price-base_cb3)-g.rounding_1) and (base_cb3-(g.rounding_2-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_2-(vip_selling_price-base_cb3)))
	 when (vip_selling_price-base_cb3) between g.rounding_1 and g.rounding_2 and (g.rounding_2-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding_1) and sd_share_pre_cashback-(base_cb3+((vip_selling_price-base_cb3)-g.rounding_1))/(case when variant8_adjusted=0 then 1 else 1.18 end)>=LEAST(variant8_gm_floor+(Slab-1)*30,variant8_gm_floor+9*30) then (base_cb3+((vip_selling_price-base_cb3)-g.rounding_1))
     when (vip_selling_price-base_cb3) between g.rounding_1 and g.rounding_2 and (g.rounding_2-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding_1) and (base_cb3-(g.rounding_2-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_2-(vip_selling_price-base_cb3)))
     
	 when (vip_selling_price-base_cb3) between g.rounding_2 and g.rounding_3 and (g.rounding_3-(vip_selling_price-base_cb3))<= ((vip_selling_price-base_cb3)-g.rounding_2) and (base_cb3-(g.rounding_3-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_3-(vip_selling_price-base_cb3)))
     when (vip_selling_price-base_cb3) between g.rounding_2 and g.rounding_3 and (g.rounding_3-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding_2) and sd_share_pre_cashback-(base_cb3+((vip_selling_price-base_cb3)-g.rounding_2))/(case when variant8_adjusted=0 then 1 else 1.18 end)>=LEAST(variant8_gm_floor+(Slab-1)*30,variant8_gm_floor+9*30) then (base_cb3+((vip_selling_price-base_cb3)-g.rounding_2))
     when (vip_selling_price-base_cb3) between g.rounding_2 and g.rounding_3 and (g.rounding_3-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding_2) and (base_cb3-(g.rounding_3-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_3-(vip_selling_price-base_cb3)))
     
	 when (vip_selling_price-base_cb3) between g.rounding_3 and g.rounding_4 and (g.rounding_4-(vip_selling_price-base_cb3))<= ((vip_selling_price-base_cb3)-g.rounding_3) and (base_cb3-(g.rounding_4-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_4-(vip_selling_price-base_cb3)))
     when (vip_selling_price-base_cb3) between g.rounding_3 and g.rounding_4 and (g.rounding_4-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding_3) and sd_share_pre_cashback-(base_cb3+((vip_selling_price-base_cb3)-g.rounding_3))/(case when variant8_adjusted=0 then 1 else 1.18 end)>=LEAST(variant8_gm_floor+(Slab-1)*30,variant8_gm_floor+9*30) then (base_cb3+((vip_selling_price-base_cb3)-g.rounding_3))
     when (vip_selling_price-base_cb3) between g.rounding_3 and g.rounding_4 and (g.rounding_4-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding_3) and (base_cb3-(g.rounding_4-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_4-(vip_selling_price-base_cb3)))
	 
	 when (vip_selling_price-base_cb3) between g.rounding_4 and g.rounding_next and (g.rounding_next-(vip_selling_price-base_cb3))<= ((vip_selling_price-base_cb3)-g.rounding_4) and (base_cb3-(g.rounding_next-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_next-(vip_selling_price-base_cb3)))
     when (vip_selling_price-base_cb3) between g.rounding_4 and g.rounding_next and (g.rounding_next-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding_4) and sd_share_pre_cashback-(base_cb3+((vip_selling_price-base_cb3)-g.rounding_4))/(case when variant8_adjusted=0 then 1 else 1.18 end)>=LEAST(variant8_gm_floor+(Slab-1)*30,variant8_gm_floor+9*30) then (base_cb3+((vip_selling_price-base_cb3)-g.rounding_4))
     when (vip_selling_price-base_cb3) between g.rounding_4 and g.rounding_next and (g.rounding_next-(vip_selling_price-base_cb3))>= ((vip_selling_price-base_cb3)-g.rounding_4) and (base_cb3-(g.rounding_next-(vip_selling_price-base_cb3)))>=0 then (base_cb3-(g.rounding_next-(vip_selling_price-base_cb3)))

     else (base_cb3) end as base_cb3_rounded,

case when (vip_selling_price-base_cb4)<79 or (vip_selling_price-base_cb4)>=1999 then  base_cb4
     when (vip_selling_price-base_cb4)= h.rounding then base_cb4
	 when (vip_selling_price-base_cb4)= h.rounding_next then base_cb4
	 
     when (vip_selling_price-base_cb4)< h.rounding_next and (h.rounding_next-(vip_selling_price-base_cb4))<= ((vip_selling_price-base_cb4)-h.rounding) and (base_cb4-(h.rounding_next-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_next-(vip_selling_price-base_cb4)))
	 when (vip_selling_price-base_cb4)< h.rounding_next and (h.rounding_next-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding) and sd_share_pre_cashback-(base_cb4+((vip_selling_price-base_cb4)-h.rounding))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb4+((vip_selling_price-base_cb4)-h.rounding))
	 when (vip_selling_price-base_cb4)< h.rounding_next and (h.rounding_next-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding) and (base_cb4-(h.rounding_next-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_next-(vip_selling_price-base_cb4)))
          
     when (vip_selling_price-base_cb4) between h.rounding and h.rounding_1 and (h.rounding_1-(vip_selling_price-base_cb4))<= ((vip_selling_price-base_cb4)-h.rounding) and (base_cb4-(h.rounding_1-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_1-(vip_selling_price-base_cb4)))
	 when (vip_selling_price-base_cb4) between h.rounding and h.rounding_1 and (h.rounding_1-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding) and sd_share_pre_cashback-(base_cb4+((vip_selling_price-base_cb4)-h.rounding))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb4+((vip_selling_price-base_cb4)-h.rounding))
     when (vip_selling_price-base_cb4) between h.rounding and h.rounding_1 and (h.rounding_1-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding) and (base_cb4-(h.rounding_1-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_1-(vip_selling_price-base_cb4)))
     
	 when (vip_selling_price-base_cb4) between h.rounding_1 and h.rounding_2 and (h.rounding_2-(vip_selling_price-base_cb4))<= ((vip_selling_price-base_cb4)-h.rounding_1) and (base_cb4-(h.rounding_2-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_2-(vip_selling_price-base_cb4)))
	 when (vip_selling_price-base_cb4) between h.rounding_1 and h.rounding_2 and (h.rounding_2-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding_1) and sd_share_pre_cashback-(base_cb4+((vip_selling_price-base_cb4)-h.rounding_1))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb4+((vip_selling_price-base_cb4)-h.rounding_1))
     when (vip_selling_price-base_cb4) between h.rounding_1 and h.rounding_2 and (h.rounding_2-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding_1) and (base_cb4-(h.rounding_2-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_2-(vip_selling_price-base_cb4)))
     
	 when (vip_selling_price-base_cb4) between h.rounding_2 and h.rounding_3 and (h.rounding_3-(vip_selling_price-base_cb4))<= ((vip_selling_price-base_cb4)-h.rounding_2) and (base_cb4-(h.rounding_3-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_3-(vip_selling_price-base_cb4)))
     when (vip_selling_price-base_cb4) between h.rounding_2 and h.rounding_3 and (h.rounding_3-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding_2) and sd_share_pre_cashback-(base_cb4+((vip_selling_price-base_cb4)-h.rounding_2))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb4+((vip_selling_price-base_cb4)-h.rounding_2))
     when (vip_selling_price-base_cb4) between h.rounding_2 and h.rounding_3 and (h.rounding_3-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding_2) and (base_cb4-(h.rounding_3-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_3-(vip_selling_price-base_cb4)))
     
	 when (vip_selling_price-base_cb4) between h.rounding_3 and h.rounding_4 and (h.rounding_4-(vip_selling_price-base_cb4))<= ((vip_selling_price-base_cb4)-h.rounding_3) and (base_cb4-(h.rounding_4-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_4-(vip_selling_price-base_cb4)))
     when (vip_selling_price-base_cb4) between h.rounding_3 and h.rounding_4 and (h.rounding_4-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding_3) and sd_share_pre_cashback-(base_cb4+((vip_selling_price-base_cb4)-h.rounding_3))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb4+((vip_selling_price-base_cb4)-h.rounding_3))
     when (vip_selling_price-base_cb4) between h.rounding_3 and h.rounding_4 and (h.rounding_4-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding_3) and (base_cb4-(h.rounding_4-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_4-(vip_selling_price-base_cb4)))
	 
	 when (vip_selling_price-base_cb4) between h.rounding_4 and h.rounding_next and (h.rounding_next-(vip_selling_price-base_cb4))<= ((vip_selling_price-base_cb4)-h.rounding_4) and (base_cb4-(h.rounding_next-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_next-(vip_selling_price-base_cb4)))
     when (vip_selling_price-base_cb4) between h.rounding_4 and h.rounding_next and (h.rounding_next-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding_4) and sd_share_pre_cashback-(base_cb4+((vip_selling_price-base_cb4)-h.rounding_4))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb4+((vip_selling_price-base_cb4)-h.rounding_4))
     when (vip_selling_price-base_cb4) between h.rounding_4 and h.rounding_next and (h.rounding_next-(vip_selling_price-base_cb4))>= ((vip_selling_price-base_cb4)-h.rounding_4) and (base_cb4-(h.rounding_next-(vip_selling_price-base_cb4)))>=0 then (base_cb4-(h.rounding_next-(vip_selling_price-base_cb4)))

     else (base_cb4) end as base_cb4_rounded, 

case when (vip_selling_price-base_cb5)<79 or (vip_selling_price-base_cb5)>=1999 then  base_cb5
     when (vip_selling_price-base_cb5)= i.rounding then base_cb5
	 when (vip_selling_price-base_cb5)= i.rounding_next then base_cb5
	 
     when (vip_selling_price-base_cb5)< i.rounding_next and (i.rounding_next-(vip_selling_price-base_cb5))<= ((vip_selling_price-base_cb5)-i.rounding) and (base_cb5-(i.rounding_next-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_next-(vip_selling_price-base_cb5)))
	 when (vip_selling_price-base_cb5)< i.rounding_next and (i.rounding_next-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding) and sd_share_pre_cashback-(base_cb5+((vip_selling_price-base_cb5)-i.rounding))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb5+((vip_selling_price-base_cb5)-i.rounding))
	 when (vip_selling_price-base_cb5)< i.rounding_next and (i.rounding_next-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding) and (base_cb5-(i.rounding_next-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_next-(vip_selling_price-base_cb5)))
          
     when (vip_selling_price-base_cb5) between i.rounding and i.rounding_1 and (i.rounding_1-(vip_selling_price-base_cb5))<= ((vip_selling_price-base_cb5)-i.rounding) and (base_cb5-(i.rounding_1-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_1-(vip_selling_price-base_cb5)))
	 when (vip_selling_price-base_cb5) between i.rounding and i.rounding_1 and (i.rounding_1-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding) and sd_share_pre_cashback-(base_cb5+((vip_selling_price-base_cb5)-i.rounding))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb5+((vip_selling_price-base_cb5)-i.rounding))
     when (vip_selling_price-base_cb5) between i.rounding and i.rounding_1 and (i.rounding_1-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding) and (base_cb5-(i.rounding_1-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_1-(vip_selling_price-base_cb5)))
     
	 when (vip_selling_price-base_cb5) between i.rounding_1 and i.rounding_2 and (i.rounding_2-(vip_selling_price-base_cb5))<= ((vip_selling_price-base_cb5)-i.rounding_1) and (base_cb5-(i.rounding_2-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_2-(vip_selling_price-base_cb5)))
	 when (vip_selling_price-base_cb5) between i.rounding_1 and i.rounding_2 and (i.rounding_2-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding_1) and sd_share_pre_cashback-(base_cb5+((vip_selling_price-base_cb5)-i.rounding_1))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb5+((vip_selling_price-base_cb5)-i.rounding_1))
     when (vip_selling_price-base_cb5) between i.rounding_1 and i.rounding_2 and (i.rounding_2-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding_1) and (base_cb5-(i.rounding_2-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_2-(vip_selling_price-base_cb5)))
     
	 when (vip_selling_price-base_cb5) between i.rounding_2 and i.rounding_3 and (i.rounding_3-(vip_selling_price-base_cb5))<= ((vip_selling_price-base_cb5)-i.rounding_2) and (base_cb5-(i.rounding_3-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_3-(vip_selling_price-base_cb5)))
     when (vip_selling_price-base_cb5) between i.rounding_2 and i.rounding_3 and (i.rounding_3-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding_2) and sd_share_pre_cashback-(base_cb5+((vip_selling_price-base_cb5)-i.rounding_2))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb5+((vip_selling_price-base_cb5)-i.rounding_2))
     when (vip_selling_price-base_cb5) between i.rounding_2 and i.rounding_3 and (i.rounding_3-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding_2) and (base_cb5-(i.rounding_3-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_3-(vip_selling_price-base_cb5)))
     
	 when (vip_selling_price-base_cb5) between i.rounding_3 and i.rounding_4 and (i.rounding_4-(vip_selling_price-base_cb5))<= ((vip_selling_price-base_cb5)-i.rounding_3) and (base_cb5-(i.rounding_4-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_4-(vip_selling_price-base_cb5)))
     when (vip_selling_price-base_cb5) between i.rounding_3 and i.rounding_4 and (i.rounding_4-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding_3) and sd_share_pre_cashback-(base_cb5+((vip_selling_price-base_cb5)-i.rounding_3))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb5+((vip_selling_price-base_cb5)-i.rounding_3))
     when (vip_selling_price-base_cb5) between i.rounding_3 and i.rounding_4 and (i.rounding_4-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding_3) and (base_cb5-(i.rounding_4-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_4-(vip_selling_price-base_cb5)))
	 
	 when (vip_selling_price-base_cb5) between i.rounding_4 and i.rounding_next and (i.rounding_next-(vip_selling_price-base_cb5))<= ((vip_selling_price-base_cb5)-i.rounding_4) and (base_cb5-(i.rounding_next-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_next-(vip_selling_price-base_cb5)))
     when (vip_selling_price-base_cb5) between i.rounding_4 and i.rounding_next and (i.rounding_next-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding_4) and sd_share_pre_cashback-(base_cb5+((vip_selling_price-base_cb5)-i.rounding_4))/(case when variant2_adjusted=0 then 1 else 1.18 end)>=LEAST(variant2_gm_floor+(Slab-1)*30,variant2_gm_floor+9*30) then (base_cb5+((vip_selling_price-base_cb5)-i.rounding_4))
     when (vip_selling_price-base_cb5) between i.rounding_4 and i.rounding_next and (i.rounding_next-(vip_selling_price-base_cb5))>= ((vip_selling_price-base_cb5)-i.rounding_4) and (base_cb5-(i.rounding_next-(vip_selling_price-base_cb5)))>=0 then (base_cb5-(i.rounding_next-(vip_selling_price-base_cb5)))

     else (base_cb5) end as base_cb5_rounded,                 

case when (vip_selling_price-sale1_cb)<79 or (vip_selling_price-sale1_cb)>=1999 then  sale1_cb
     when (vip_selling_price-sale1_cb)= b.rounding then sale1_cb
	 when (vip_selling_price-sale1_cb)= b.rounding_next then sale1_cb
	 
     when (vip_selling_price-sale1_cb)< b.rounding_next and (b.rounding_next-(vip_selling_price-sale1_cb))<= ((vip_selling_price-sale1_cb)-b.rounding) and (sale1_cb-(b.rounding_next-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_next-(vip_selling_price-sale1_cb)))
	 when (vip_selling_price-sale1_cb)< b.rounding_next and (b.rounding_next-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding) and sd_share_pre_cashback-(sale1_cb+((vip_selling_price-sale1_cb)-b.rounding))/(case when variant3_adjusted=0 then 1 else 1.18 end)>=LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30) then (sale1_cb+((vip_selling_price-sale1_cb)-b.rounding))
	 when (vip_selling_price-sale1_cb)< b.rounding_next and (b.rounding_next-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding) and (sale1_cb-(b.rounding_next-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_next-(vip_selling_price-sale1_cb)))
          
     when (vip_selling_price-sale1_cb) between b.rounding and b.rounding_1 and (b.rounding_1-(vip_selling_price-sale1_cb))<= ((vip_selling_price-sale1_cb)-b.rounding) and (sale1_cb-(b.rounding_1-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_1-(vip_selling_price-sale1_cb)))
	 when (vip_selling_price-sale1_cb) between b.rounding and b.rounding_1 and (b.rounding_1-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding) and sd_share_pre_cashback-(sale1_cb+((vip_selling_price-sale1_cb)-b.rounding))/(case when variant3_adjusted=0 then 1 else 1.18 end)>=LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30) then (sale1_cb+((vip_selling_price-sale1_cb)-b.rounding))
     when (vip_selling_price-sale1_cb) between b.rounding and b.rounding_1 and (b.rounding_1-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding) and (sale1_cb-(b.rounding_1-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_1-(vip_selling_price-sale1_cb)))
     
	 when (vip_selling_price-sale1_cb) between b.rounding_1 and b.rounding_2 and (b.rounding_2-(vip_selling_price-sale1_cb))<= ((vip_selling_price-sale1_cb)-b.rounding_1) and (sale1_cb-(b.rounding_2-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_2-(vip_selling_price-sale1_cb)))
	 when (vip_selling_price-sale1_cb) between b.rounding_1 and b.rounding_2 and (b.rounding_2-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding_1) and sd_share_pre_cashback-(sale1_cb+((vip_selling_price-sale1_cb)-b.rounding_1))/(case when variant3_adjusted=0 then 1 else 1.18 end)>=LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30) then (sale1_cb+((vip_selling_price-sale1_cb)-b.rounding_1))
     when (vip_selling_price-sale1_cb) between b.rounding_1 and b.rounding_2 and (b.rounding_2-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding_1) and (sale1_cb-(b.rounding_2-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_2-(vip_selling_price-sale1_cb)))
     
	 when (vip_selling_price-sale1_cb) between b.rounding_2 and b.rounding_3 and (b.rounding_3-(vip_selling_price-sale1_cb))<= ((vip_selling_price-sale1_cb)-b.rounding_2) and (sale1_cb-(b.rounding_3-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_3-(vip_selling_price-sale1_cb)))
     when (vip_selling_price-sale1_cb) between b.rounding_2 and b.rounding_3 and (b.rounding_3-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding_2) and sd_share_pre_cashback-(sale1_cb+((vip_selling_price-sale1_cb)-b.rounding_2))/(case when variant3_adjusted=0 then 1 else 1.18 end)>=LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30) then (sale1_cb+((vip_selling_price-sale1_cb)-b.rounding_2))
     when (vip_selling_price-sale1_cb) between b.rounding_2 and b.rounding_3 and (b.rounding_3-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding_2) and (sale1_cb-(b.rounding_3-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_3-(vip_selling_price-sale1_cb)))
     
	 when (vip_selling_price-sale1_cb) between b.rounding_3 and b.rounding_4 and (b.rounding_4-(vip_selling_price-sale1_cb))<= ((vip_selling_price-sale1_cb)-b.rounding_3) and (sale1_cb-(b.rounding_4-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_4-(vip_selling_price-sale1_cb)))
     when (vip_selling_price-sale1_cb) between b.rounding_3 and b.rounding_4 and (b.rounding_4-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding_3) and sd_share_pre_cashback-(sale1_cb+((vip_selling_price-sale1_cb)-b.rounding_3))/(case when variant3_adjusted=0 then 1 else 1.18 end)>=LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30) then (sale1_cb+((vip_selling_price-sale1_cb)-b.rounding_3))
     when (vip_selling_price-sale1_cb) between b.rounding_3 and b.rounding_4 and (b.rounding_4-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding_3) and (sale1_cb-(b.rounding_4-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_4-(vip_selling_price-sale1_cb)))
	 
	 when (vip_selling_price-sale1_cb) between b.rounding_4 and b.rounding_next and (b.rounding_next-(vip_selling_price-sale1_cb))<= ((vip_selling_price-sale1_cb)-b.rounding_4) and (sale1_cb-(b.rounding_next-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_next-(vip_selling_price-sale1_cb)))
     when (vip_selling_price-sale1_cb) between b.rounding_4 and b.rounding_next and (b.rounding_next-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding_4) and sd_share_pre_cashback-(sale1_cb+((vip_selling_price-sale1_cb)-b.rounding_4))/(case when variant3_adjusted=0 then 1 else 1.18 end)>=LEAST(variant3_gm_floor+(Slab-1)*30,variant3_gm_floor+9*30) then (sale1_cb+((vip_selling_price-sale1_cb)-b.rounding_4))
     when (vip_selling_price-sale1_cb) between b.rounding_4 and b.rounding_next and (b.rounding_next-(vip_selling_price-sale1_cb))>= ((vip_selling_price-sale1_cb)-b.rounding_4) and (sale1_cb-(b.rounding_next-(vip_selling_price-sale1_cb)))>=0 then (sale1_cb-(b.rounding_next-(vip_selling_price-sale1_cb)))

     else (sale1_cb) end as sale1_cb_rounded,
 
case when (vip_selling_price-sale2_cb)<79 or (vip_selling_price-sale2_cb)>=1999 then  sale2_cb
     when (vip_selling_price-sale2_cb)= c.rounding then sale2_cb
	 when (vip_selling_price-sale2_cb)= c.rounding_next then sale2_cb
	 
     when (vip_selling_price-sale2_cb)< c.rounding_next and (c.rounding_next-(vip_selling_price-sale2_cb))<= ((vip_selling_price-sale2_cb)-c.rounding) and (sale2_cb-(c.rounding_next-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_next-(vip_selling_price-sale2_cb)))
	 when (vip_selling_price-sale2_cb)< c.rounding_next and (c.rounding_next-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding) and sd_share_pre_cashback-(sale2_cb+((vip_selling_price-sale2_cb)-c.rounding))/(case when variant4_adjusted=0 then 1 else 1.18 end)>=LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30) then (sale2_cb+((vip_selling_price-sale2_cb)-c.rounding))
	 when (vip_selling_price-sale2_cb)< c.rounding_next and (c.rounding_next-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding) and (sale2_cb-(c.rounding_next-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_next-(vip_selling_price-sale2_cb)))
          
     when (vip_selling_price-sale2_cb) between c.rounding and c.rounding_1 and (c.rounding_1-(vip_selling_price-sale2_cb))<= ((vip_selling_price-sale2_cb)-c.rounding) and (sale2_cb-(c.rounding_1-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_1-(vip_selling_price-sale2_cb)))
	 when (vip_selling_price-sale2_cb) between c.rounding and c.rounding_1 and (c.rounding_1-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding) and sd_share_pre_cashback-(sale2_cb+((vip_selling_price-sale2_cb)-c.rounding))/(case when variant4_adjusted=0 then 1 else 1.18 end)>=LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30) then (sale2_cb+((vip_selling_price-sale2_cb)-c.rounding))
     when (vip_selling_price-sale2_cb) between c.rounding and c.rounding_1 and (c.rounding_1-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding) and (sale2_cb-(c.rounding_1-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_1-(vip_selling_price-sale2_cb)))
     
	 when (vip_selling_price-sale2_cb) between c.rounding_1 and c.rounding_2 and (c.rounding_2-(vip_selling_price-sale2_cb))<= ((vip_selling_price-sale2_cb)-c.rounding_1) and (sale2_cb-(c.rounding_2-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_2-(vip_selling_price-sale2_cb)))
	 when (vip_selling_price-sale2_cb) between c.rounding_1 and c.rounding_2 and (c.rounding_2-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding_1) and sd_share_pre_cashback-(sale2_cb+((vip_selling_price-sale2_cb)-c.rounding_1))/(case when variant4_adjusted=0 then 1 else 1.18 end)>=LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30) then (sale2_cb+((vip_selling_price-sale2_cb)-c.rounding_1))
     when (vip_selling_price-sale2_cb) between c.rounding_1 and c.rounding_2 and (c.rounding_2-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding_1) and (sale2_cb-(c.rounding_2-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_2-(vip_selling_price-sale2_cb)))
     
	 when (vip_selling_price-sale2_cb) between c.rounding_2 and c.rounding_3 and (c.rounding_3-(vip_selling_price-sale2_cb))<= ((vip_selling_price-sale2_cb)-c.rounding_2) and (sale2_cb-(c.rounding_3-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_3-(vip_selling_price-sale2_cb)))
     when (vip_selling_price-sale2_cb) between c.rounding_2 and c.rounding_3 and (c.rounding_3-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding_2) and sd_share_pre_cashback-(sale2_cb+((vip_selling_price-sale2_cb)-c.rounding_2))/(case when variant4_adjusted=0 then 1 else 1.18 end)>=LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30) then (sale2_cb+((vip_selling_price-sale2_cb)-c.rounding_2))
     when (vip_selling_price-sale2_cb) between c.rounding_2 and c.rounding_3 and (c.rounding_3-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding_2) and (sale2_cb-(c.rounding_3-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_3-(vip_selling_price-sale2_cb)))
     
	 when (vip_selling_price-sale2_cb) between c.rounding_3 and c.rounding_4 and (c.rounding_4-(vip_selling_price-sale2_cb))<= ((vip_selling_price-sale2_cb)-c.rounding_3) and (sale2_cb-(c.rounding_4-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_4-(vip_selling_price-sale2_cb)))
     when (vip_selling_price-sale2_cb) between c.rounding_3 and c.rounding_4 and (c.rounding_4-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding_3) and sd_share_pre_cashback-(sale2_cb+((vip_selling_price-sale2_cb)-c.rounding_3))/(case when variant4_adjusted=0 then 1 else 1.18 end)>=LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30) then (sale2_cb+((vip_selling_price-sale2_cb)-c.rounding_3))
     when (vip_selling_price-sale2_cb) between c.rounding_3 and c.rounding_4 and (c.rounding_4-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding_3) and (sale2_cb-(c.rounding_4-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_4-(vip_selling_price-sale2_cb)))
	 
	 when (vip_selling_price-sale2_cb) between c.rounding_4 and c.rounding_next and (c.rounding_next-(vip_selling_price-sale2_cb))<= ((vip_selling_price-sale2_cb)-c.rounding_4) and (sale2_cb-(c.rounding_next-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_next-(vip_selling_price-sale2_cb)))
     when (vip_selling_price-sale2_cb) between c.rounding_4 and c.rounding_next and (c.rounding_next-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding_4) and sd_share_pre_cashback-(sale2_cb+((vip_selling_price-sale2_cb)-c.rounding_4))/(case when variant4_adjusted=0 then 1 else 1.18 end)>=LEAST(variant4_gm_floor+(Slab-1)*30,variant4_gm_floor+9*30) then (sale2_cb+((vip_selling_price-sale2_cb)-c.rounding_4))
     when (vip_selling_price-sale2_cb) between c.rounding_4 and c.rounding_next and (c.rounding_next-(vip_selling_price-sale2_cb))>= ((vip_selling_price-sale2_cb)-c.rounding_4) and (sale2_cb-(c.rounding_next-(vip_selling_price-sale2_cb)))>=0 then (sale2_cb-(c.rounding_next-(vip_selling_price-sale2_cb)))

     else (sale2_cb) end as sale2_cb_rounded,     
    
     
case when (vip_selling_price-sale3_cb)<79 or (vip_selling_price-sale3_cb)>=1999 then  sale3_cb
     when (vip_selling_price-sale3_cb)= d.rounding then sale3_cb
	 when (vip_selling_price-sale3_cb)= d.rounding_next then sale3_cb
	 
     when (vip_selling_price-sale3_cb)< d.rounding_next and (d.rounding_next-(vip_selling_price-sale3_cb))<= ((vip_selling_price-sale3_cb)-d.rounding) and (sale3_cb-(d.rounding_next-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_next-(vip_selling_price-sale3_cb)))
	 when (vip_selling_price-sale3_cb)< d.rounding_next and (d.rounding_next-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding) and sd_share_pre_cashback-(sale3_cb+((vip_selling_price-sale3_cb)-d.rounding))/(case when variant5_adjusted=0 then 1 else 1.18 end)>=LEAST(variant5_gm_floor+(Slab-1)*30,variant5_gm_floor+9*30) then (sale3_cb+((vip_selling_price-sale3_cb)-d.rounding))
	 when (vip_selling_price-sale3_cb)< d.rounding_next and (d.rounding_next-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding) and (sale3_cb-(d.rounding_next-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_next-(vip_selling_price-sale3_cb)))
          
     when (vip_selling_price-sale3_cb) between d.rounding and d.rounding_1 and (d.rounding_1-(vip_selling_price-sale3_cb))<= ((vip_selling_price-sale3_cb)-d.rounding) and (sale3_cb-(d.rounding_1-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_1-(vip_selling_price-sale3_cb)))
	 when (vip_selling_price-sale3_cb) between d.rounding and d.rounding_1 and (d.rounding_1-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding) and sd_share_pre_cashback-(sale3_cb+((vip_selling_price-sale3_cb)-d.rounding))/(case when variant5_adjusted=0 then 1 else 1.18 end)>=LEAST(variant5_gm_floor+(Slab-1)*30,variant5_gm_floor+9*30) then (sale3_cb+((vip_selling_price-sale3_cb)-d.rounding))
     when (vip_selling_price-sale3_cb) between d.rounding and d.rounding_1 and (d.rounding_1-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding) and (sale3_cb-(d.rounding_1-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_1-(vip_selling_price-sale3_cb)))
     
	 when (vip_selling_price-sale3_cb) between d.rounding_1 and d.rounding_2 and (d.rounding_2-(vip_selling_price-sale3_cb))<= ((vip_selling_price-sale3_cb)-d.rounding_1) and (sale3_cb-(d.rounding_2-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_2-(vip_selling_price-sale3_cb)))
	 when (vip_selling_price-sale3_cb) between d.rounding_1 and d.rounding_2 and (d.rounding_2-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding_1) and sd_share_pre_cashback-(sale3_cb+((vip_selling_price-sale3_cb)-d.rounding_1))/(case when variant5_adjusted=0 then 1 else 1.18 end)>=LEAST(variant5_gm_floor+(Slab-1)*30,variant5_gm_floor+9*30) then (sale3_cb+((vip_selling_price-sale3_cb)-d.rounding_1))
     when (vip_selling_price-sale3_cb) between d.rounding_1 and d.rounding_2 and (d.rounding_2-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding_1) and (sale3_cb-(d.rounding_2-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_2-(vip_selling_price-sale3_cb)))
     
	 when (vip_selling_price-sale3_cb) between d.rounding_2 and d.rounding_3 and (d.rounding_3-(vip_selling_price-sale3_cb))<= ((vip_selling_price-sale3_cb)-d.rounding_2) and (sale3_cb-(d.rounding_3-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_3-(vip_selling_price-sale3_cb)))
     when (vip_selling_price-sale3_cb) between d.rounding_2 and d.rounding_3 and (d.rounding_3-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding_2) and sd_share_pre_cashback-(sale3_cb+((vip_selling_price-sale3_cb)-d.rounding_2))/(case when variant5_adjusted=0 then 1 else 1.18 end)>=LEAST(variant5_gm_floor+(Slab-1)*30,variant5_gm_floor+9*30) then (sale3_cb+((vip_selling_price-sale3_cb)-d.rounding_2))
     when (vip_selling_price-sale3_cb) between d.rounding_2 and d.rounding_3 and (d.rounding_3-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding_2) and (sale3_cb-(d.rounding_3-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_3-(vip_selling_price-sale3_cb)))
     
	 when (vip_selling_price-sale3_cb) between d.rounding_3 and d.rounding_4 and (d.rounding_4-(vip_selling_price-sale3_cb))<= ((vip_selling_price-sale3_cb)-d.rounding_3) and (sale3_cb-(d.rounding_4-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_4-(vip_selling_price-sale3_cb)))
     when (vip_selling_price-sale3_cb) between d.rounding_3 and d.rounding_4 and (d.rounding_4-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding_3) and sd_share_pre_cashback-(sale3_cb+((vip_selling_price-sale3_cb)-d.rounding_3))/(case when variant5_adjusted=0 then 1 else 1.18 end)>=LEAST(variant5_gm_floor+(Slab-1)*30,variant5_gm_floor+9*30) then (sale3_cb+((vip_selling_price-sale3_cb)-d.rounding_3))
     when (vip_selling_price-sale3_cb) between d.rounding_3 and d.rounding_4 and (d.rounding_4-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding_3) and (sale3_cb-(d.rounding_4-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_4-(vip_selling_price-sale3_cb)))
	 
	 when (vip_selling_price-sale3_cb) between d.rounding_4 and d.rounding_next and (d.rounding_next-(vip_selling_price-sale3_cb))<= ((vip_selling_price-sale3_cb)-d.rounding_4) and (sale3_cb-(d.rounding_next-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_next-(vip_selling_price-sale3_cb)))
     when (vip_selling_price-sale3_cb) between d.rounding_4 and d.rounding_next and (d.rounding_next-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding_4) and sd_share_pre_cashback-(sale3_cb+((vip_selling_price-sale3_cb)-d.rounding_4))/(case when variant5_adjusted=0 then 1 else 1.18 end)>=LEAST(variant5_gm_floor+(Slab-1)*30,variant5_gm_floor+9*30) then (sale3_cb+((vip_selling_price-sale3_cb)-d.rounding_4))
     when (vip_selling_price-sale3_cb) between d.rounding_4 and d.rounding_next and (d.rounding_next-(vip_selling_price-sale3_cb))>= ((vip_selling_price-sale3_cb)-d.rounding_4) and (sale3_cb-(d.rounding_next-(vip_selling_price-sale3_cb)))>=0 then (sale3_cb-(d.rounding_next-(vip_selling_price-sale3_cb)))

     else (sale3_cb) end as sale3_cb_rounded,      

case when (vip_selling_price-sale4_cb)<79 or (vip_selling_price-sale4_cb)>=1999 then  sale4_cb
     when (vip_selling_price-sale4_cb)= j.rounding then sale4_cb
	 when (vip_selling_price-sale4_cb)= j.rounding_next then sale4_cb
	 
     when (vip_selling_price-sale4_cb)< j.rounding_next and (j.rounding_next-(vip_selling_price-sale4_cb))<= ((vip_selling_price-sale4_cb)-j.rounding) and (sale4_cb-(j.rounding_next-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_next-(vip_selling_price-sale4_cb)))
	 when (vip_selling_price-sale4_cb)< j.rounding_next and (j.rounding_next-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding) and sd_share_pre_cashback-(sale4_cb+((vip_selling_price-sale4_cb)-j.rounding))/(case when variant6_adjusted=0 then 1 else 1.18 end)>=LEAST(variant6_gm_floor+(Slab-1)*30,variant6_gm_floor+9*30) then (sale4_cb+((vip_selling_price-sale4_cb)-j.rounding))
	 when (vip_selling_price-sale4_cb)< j.rounding_next and (j.rounding_next-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding) and (sale4_cb-(j.rounding_next-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_next-(vip_selling_price-sale4_cb)))
          
     when (vip_selling_price-sale4_cb) between j.rounding and j.rounding_1 and (j.rounding_1-(vip_selling_price-sale4_cb))<= ((vip_selling_price-sale4_cb)-j.rounding) and (sale4_cb-(j.rounding_1-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_1-(vip_selling_price-sale4_cb)))
	 when (vip_selling_price-sale4_cb) between j.rounding and j.rounding_1 and (j.rounding_1-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding) and sd_share_pre_cashback-(sale4_cb+((vip_selling_price-sale4_cb)-j.rounding))/(case when variant6_adjusted=0 then 1 else 1.18 end)>=LEAST(variant6_gm_floor+(Slab-1)*30,variant6_gm_floor+9*30) then (sale4_cb+((vip_selling_price-sale4_cb)-j.rounding))
     when (vip_selling_price-sale4_cb) between j.rounding and j.rounding_1 and (j.rounding_1-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding) and (sale4_cb-(j.rounding_1-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_1-(vip_selling_price-sale4_cb)))
     
	 when (vip_selling_price-sale4_cb) between j.rounding_1 and j.rounding_2 and (j.rounding_2-(vip_selling_price-sale4_cb))<= ((vip_selling_price-sale4_cb)-j.rounding_1) and (sale4_cb-(j.rounding_2-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_2-(vip_selling_price-sale4_cb)))
	 when (vip_selling_price-sale4_cb) between j.rounding_1 and j.rounding_2 and (j.rounding_2-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding_1) and sd_share_pre_cashback-(sale4_cb+((vip_selling_price-sale4_cb)-j.rounding_1))/(case when variant6_adjusted=0 then 1 else 1.18 end)>=LEAST(variant6_gm_floor+(Slab-1)*30,variant6_gm_floor+9*30) then (sale4_cb+((vip_selling_price-sale4_cb)-j.rounding_1))
     when (vip_selling_price-sale4_cb) between j.rounding_1 and j.rounding_2 and (j.rounding_2-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding_1) and (sale4_cb-(j.rounding_2-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_2-(vip_selling_price-sale4_cb)))
     
	 when (vip_selling_price-sale4_cb) between j.rounding_2 and j.rounding_3 and (j.rounding_3-(vip_selling_price-sale4_cb))<= ((vip_selling_price-sale4_cb)-j.rounding_2) and (sale4_cb-(j.rounding_3-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_3-(vip_selling_price-sale4_cb)))
     when (vip_selling_price-sale4_cb) between j.rounding_2 and j.rounding_3 and (j.rounding_3-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding_2) and sd_share_pre_cashback-(sale4_cb+((vip_selling_price-sale4_cb)-j.rounding_2))/(case when variant6_adjusted=0 then 1 else 1.18 end)>=LEAST(variant6_gm_floor+(Slab-1)*30,variant6_gm_floor+9*30) then (sale4_cb+((vip_selling_price-sale4_cb)-j.rounding_2))
     when (vip_selling_price-sale4_cb) between j.rounding_2 and j.rounding_3 and (j.rounding_3-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding_2) and (sale4_cb-(j.rounding_3-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_3-(vip_selling_price-sale4_cb)))
     
	 when (vip_selling_price-sale4_cb) between j.rounding_3 and j.rounding_4 and (j.rounding_4-(vip_selling_price-sale4_cb))<= ((vip_selling_price-sale4_cb)-j.rounding_3) and (sale4_cb-(j.rounding_4-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_4-(vip_selling_price-sale4_cb)))
     when (vip_selling_price-sale4_cb) between j.rounding_3 and j.rounding_4 and (j.rounding_4-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding_3) and sd_share_pre_cashback-(sale4_cb+((vip_selling_price-sale4_cb)-j.rounding_3))/(case when variant6_adjusted=0 then 1 else 1.18 end)>=LEAST(variant6_gm_floor+(Slab-1)*30,variant6_gm_floor+9*30) then (sale4_cb+((vip_selling_price-sale4_cb)-j.rounding_3))
     when (vip_selling_price-sale4_cb) between j.rounding_3 and j.rounding_4 and (j.rounding_4-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding_3) and (sale4_cb-(j.rounding_4-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_4-(vip_selling_price-sale4_cb)))
	 
	 when (vip_selling_price-sale4_cb) between j.rounding_4 and j.rounding_next and (j.rounding_next-(vip_selling_price-sale4_cb))<= ((vip_selling_price-sale4_cb)-j.rounding_4) and (sale4_cb-(j.rounding_next-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_next-(vip_selling_price-sale4_cb)))
     when (vip_selling_price-sale4_cb) between j.rounding_4 and j.rounding_next and (j.rounding_next-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding_4) and sd_share_pre_cashback-(sale4_cb+((vip_selling_price-sale4_cb)-j.rounding_4))/(case when variant6_adjusted=0 then 1 else 1.18 end)>=LEAST(variant6_gm_floor+(Slab-1)*30,variant6_gm_floor+9*30) then (sale4_cb+((vip_selling_price-sale4_cb)-j.rounding_4))
     when (vip_selling_price-sale4_cb) between j.rounding_4 and j.rounding_next and (j.rounding_next-(vip_selling_price-sale4_cb))>= ((vip_selling_price-sale4_cb)-j.rounding_4) and (sale4_cb-(j.rounding_next-(vip_selling_price-sale4_cb)))>=0 then (sale4_cb-(j.rounding_next-(vip_selling_price-sale4_cb)))

     else (sale4_cb) end as sale4_cb_rounded      
        
                from analytics_pricing.base_tables3_cb m
                left join analytics_logistics.rounding_exc_2 a on (m.vip_selling_price-m.base_cb) >= a.rounding and (m.vip_selling_price-m.base_cb) <a.rounding_next
                left join analytics_logistics.rounding_exc_2 b on (m.vip_selling_price-m.sale1_cb) >= b.rounding and (m.vip_selling_price-m.sale1_cb) <b.rounding_next
                left join analytics_logistics.rounding_exc_2 c on (m.vip_selling_price-m.sale2_cb) >= c.rounding and (m.vip_selling_price-m.sale2_cb) <c.rounding_next
                left join analytics_logistics.rounding_exc_2 d on (m.vip_selling_price-m.sale3_cb) >= d.rounding and (m.vip_selling_price-m.sale3_cb) <d.rounding_next
                left join analytics_logistics.rounding_exc_2 e on (m.vip_selling_price-m.base_cb1) >= e.rounding and (m.vip_selling_price-m.base_cb1) <e.rounding_next
                left join analytics_logistics.rounding_exc_2 f on (m.vip_selling_price-m.base_cb2) >= f.rounding and (m.vip_selling_price-m.base_cb2) <f.rounding_next 
                left join analytics_logistics.rounding_exc_2 g on (m.vip_selling_price-m.base_cb3) >= g.rounding and (m.vip_selling_price-m.base_cb3) <g.rounding_next 
                left join analytics_logistics.rounding_exc_2 h on (m.vip_selling_price-m.base_cb4) >= h.rounding and (m.vip_selling_price-m.base_cb4) <h.rounding_next 
                left join analytics_logistics.rounding_exc_2 i on (m.vip_selling_price-m.base_cb5) >= i.rounding and (m.vip_selling_price-m.base_cb5) <i.rounding_next 
                left join analytics_logistics.rounding_exc_2 j on (m.vip_selling_price-m.sale4_cb) >= j.rounding and (m.vip_selling_price-m.sale4_cb) <j.rounding_next                                                                
--where supc='SDL006123294'
;


UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step6';

INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step7',(SELECT NOW()),NULL;


TRUNCATE TABLE analytics_pricing.Cashback_list_final;
insert into analytics_pricing.Cashback_list_final 
select a.*,
case when  (lastD15Sale > 0 or Sold_7_flag_pog = 1) then 1
     when Sold_30_flag_pog=1 then 2
     when top_vendor=1 then 3
     when category_request_cb=1 then 4
     else 5 end as 'cb_update_flag',
     
case when Non_Sale_Variant='No_CB' then 0 
     when Non_Sale_Variant='BAU_CB' then base_cb_rounded
     else 0 end as List_A_CB,     
 
case when Non_Sale_Variant='No_CB' then 0 
     when Non_Sale_Variant='BAU_CB' then base_cb_rounded
     else 0 end as List_B_CB,

case when is_hard_exception=1 and sale_variant='No_CB' then 0
     when vendor_code in ('S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 'S41fb7', 'Sfa20d', '8e1ea4', 'Se649f', 'Sf113e', 'S2ac6a', 'S80132', 'Sd1243', 'S009e9', 'Sc27c9', 'S4f0d0','S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 's41fb7', 'sfa20d', '8e1ea4', 'se649f', 'sf113e', 's2ac6a', 's80132', 'sd1243', 's009e9', 'Sc27c9', 'S4f0d0', 'S9176a', 'S7a0c0', 'S10901', 'S3f400')
     then base_cb3_rounded /*approve_by_saurabh Pl_supc*/
     when bad_seller=1 then sale4_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)<0.03 and a.selling_price between 200 and 600 then sale4_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)>=0.03 then sale3_cb_rounded
     when sale_participation='Yes' then sale3_cb_rounded
     when sale_participation in ('No','Other') and high_cb_pog=1 then sale4_cb_rounded 
     when sale_participation in ('No','Other') then sale4_cb_rounded
     else base_cb_rounded end as List_C_CB,
          
case when is_hard_exception=1 and sale_variant='No_CB' then 0
     when vendor_code in ('S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 'S41fb7', 'Sfa20d', '8e1ea4', 'Se649f', 'Sf113e', 'S2ac6a', 'S80132', 'Sd1243', 'S009e9', 'Sc27c9', 'S4f0d0','S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 's41fb7', 'sfa20d', '8e1ea4', 'se649f', 'sf113e', 's2ac6a', 's80132', 'sd1243', 's009e9', 'Sc27c9', 'S4f0d0', 'S9176a', 'S7a0c0', 'S10901', 'S3f400')
     then base_cb3_rounded /*approve_by_saurabh Pl_supc*/
     when bad_seller=1 then sale2_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)<0.03 and a.selling_price between 200 and 600 then sale2_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)>=0.03 then sale1_cb_rounded
     when sale_participation='Yes' then sale1_cb_rounded
     when sale_participation in ('No','Other') and high_cb_pog=1 then sale2_cb_rounded 
     when sale_participation in ('No','Other') then sale2_cb_rounded
     else base_cb_rounded end as List_D_CB,
     
case when Non_Sale_Variant='No_CB' then 0 
     when Non_Sale_Variant='BAU_CB' and seller_zone in ('Yellow-Bad','Red') then base_cb2_rounded
     when Non_Sale_Variant='BAU_CB' and seller_zone='Yellow-Good' then base_cb3_rounded
	 when Non_Sale_Variant='BAU_CB' and seller_zone='Green' then base_cb_rounded
     when Non_Sale_Variant='BAU_CB' then base_cb3_rounded
     else 0 end as List_E_CB,   
        
case when is_hard_exception=1 and sale_variant='No_CB' then 0
     when seller_zone='Red' then (case when Non_Sale_Variant='No_CB' then 0 when Non_Sale_Variant='BAU_CB' then base_cb3_rounded else 0 end)
     when seller_zone in ('Yellow-Bad','Yellow-Good') then 
          (case when is_hard_exception=1 and sale_variant='No_CB' then 0
     when vendor_code in ('S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 'S41fb7', 'Sfa20d', '8e1ea4', 'Se649f', 'Sf113e', 'S2ac6a', 'S80132', 'Sd1243', 'S009e9', 'Sc27c9', 'S4f0d0','S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 's41fb7', 'sfa20d', '8e1ea4', 'se649f', 'sf113e', 's2ac6a', 's80132', 'sd1243', 's009e9', 'Sc27c9', 'S4f0d0', 'S9176a', 'S7a0c0', 'S10901', 'S3f400')
     then base_cb3_rounded /*approve_by_saurabh Pl_supc*/
     when bad_seller=1 then sale4_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)<0.03 and a.selling_price between 200 and 600 then sale4_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)>=0.03 then sale3_cb_rounded
     when sale_participation='Yes' then sale3_cb_rounded
     when sale_participation in ('No','Other') and high_cb_pog=1 then sale4_cb_rounded 
     when sale_participation in ('No','Other') then sale4_cb_rounded
     else base_cb_rounded end)
     when seller_zone='Green' then 
          (case when is_hard_exception=1 and sale_variant='No_CB' then 0
     when vendor_code in ('S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 'S41fb7', 'Sfa20d', '8e1ea4', 'Se649f', 'Sf113e', 'S2ac6a', 'S80132', 'Sd1243', 'S009e9', 'Sc27c9', 'S4f0d0','S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 's41fb7', 'sfa20d', '8e1ea4', 'se649f', 'sf113e', 's2ac6a', 's80132', 'sd1243', 's009e9', 'Sc27c9', 'S4f0d0', 'S9176a', 'S7a0c0', 'S10901', 'S3f400')
     then base_cb3_rounded /*approve_by_saurabh Pl_supc*/
     when bad_seller=1 then sale2_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)<0.03 and a.selling_price between 200 and 600 then sale2_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)>=0.03 then sale1_cb_rounded
     when sale_participation='Yes' then sale1_cb_rounded
     when sale_participation in ('No','Other') and high_cb_pog=1 then sale2_cb_rounded 
     when sale_participation in ('No','Other') then sale2_cb_rounded
     else base_cb_rounded end)
     else   (case when is_hard_exception=1 and sale_variant='No_CB' then 0
     when vendor_code in ('S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 'S41fb7', 'Sfa20d', '8e1ea4', 'Se649f', 'Sf113e', 'S2ac6a', 'S80132', 'Sd1243', 'S009e9', 'Sc27c9', 'S4f0d0','S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 's41fb7', 'sfa20d', '8e1ea4', 'se649f', 'sf113e', 's2ac6a', 's80132', 'sd1243', 's009e9', 'Sc27c9', 'S4f0d0', 'S9176a', 'S7a0c0', 'S10901', 'S3f400')
     then base_cb3_rounded /*approve_by_saurabh Pl_supc*/
     when bad_seller=1 then sale4_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)<0.03 and a.selling_price between 200 and 600 then sale4_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)>=0.03 then sale3_cb_rounded
     when sale_participation='Yes' then sale3_cb_rounded
     when sale_participation in ('No','Other') and high_cb_pog=1 then sale4_cb_rounded 
     when sale_participation in ('No','Other') then sale4_cb_rounded
     else base_cb_rounded end) end as List_F_CB,
     
case when Non_Sale_Variant='No_CB' then 0 
     when Non_Sale_Variant='BAU_CB' then base_cb_rounded
     else 0 end as List_G_CB,
     
case when Non_Sale_Variant='No_CB' then 0 
     when Non_Sale_Variant='BAU_CB' then base_cb2_rounded
     else 0 end as List_H_CB,
     
case when Non_Sale_Variant='No_CB' then 0 
     when Non_Sale_Variant='BAU_CB' then base_cb_rounded
     else 0 end as List_I_CB,
     
case when is_hard_exception=1 and sale_variant='No_CB' then 0
     when vendor_code in ('S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 'S41fb7', 'Sfa20d', '8e1ea4',  'Se649f', 'Sf113e', 'S2ac6a', 'S80132', 'Sd1243', 'S009e9', 'Sc27c9', 'S4f0d0','S504c2', 'Sdd142', 'S13321', 'S1b016', 'Sb1ba9', 'S45396', 'S9ed95', 'Se430c', 'S25953', 'S12cb0', 'Sc5861', 'S8332e', 'Se6ee3', 'S2ee1d', 's41fb7', 'sfa20d', '8e1ea4',  'se649f', 'sf113e', 's2ac6a', 's80132', 'sd1243', 's009e9', 'Sc27c9', 'S4f0d0', 'S9176a', 'S7a0c0', 'S10901', 'S3f400')
     then base_cb_rounded /*approve_by_saurabh Pl_supc*/
     when bad_seller=1 then sale4_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)<0.03 and a.selling_price between 200 and 600 then sale4_cb_rounded
     when sale_participation='Yes' and (ifnull(bid,0)/100000000)>=0.03 then sale3_cb_rounded
     when sale_participation='Yes' then sale3_cb_rounded
     when sale_participation in ('No','Other') and high_cb_pog=1 then sale4_cb_rounded 
     when sale_participation in ('No','Other') then sale4_cb_rounded
     else base_cb_rounded end as List_J_CB

from analytics_pricing.Cashback_list a
--where a.supc='SDL006123294'
;


UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step7';

INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step8',(SELECT NOW()),NULL;

/*drop table if exists sd_comp_price;
create local temp table sd_comp_price on commit preserve rows as
(
select distinct
s.supc,
s.vendor_code,
s.product_sid,
s.vendor_sid,
s.selling_price,
s.List_B_CB,
s.sd_share_pre_cashback,
s.variant1_gm_floor,
s.variant1_adjusted,
s.slab,
s.selling_price-s.List_B_CB as 'OP',
b.pog,
b.Comp_min_price,
((b.Comp_min_price)-(s.selling_price-s.List_B_CB))*100/(s.selling_price-s.List_B_CB) as 'Percent_decrease'
from
(
select distinct
cp.*,
a.sp as 'Comp_min_price'
from analytics_logistics.comp_pogs_value cp
join(
select  distinct
pog,
min(selling_price) as 'sp'
from analytics_logistics.comp_pogs_value
where store IN ('Amazon_app','Flipkart_app')
group by 1
)a on a.pog = cp.pog
where cp.store = 'SnapDeal_app'
)b 
join analytics_pricing.Cashback_list_final  s on s.pog_id = b.pog 
where (s.selling_price-s.List_B_CB )<b.Comp_min_price
);



drop table if exists sd_comp_price_cb_final;
create local temp table sd_comp_price_cb_final on commit preserve rows as
(
select 
s.*,
case when  (Comp_min_price - OP) <= 20 then List_B_CB
     when (Comp_min_price - OP) between 20 and 50 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.10*List_B_CB)))>20  then List_B_CB-(0.10*List_B_CB)
     when (Comp_min_price - OP) between 20 and 50 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.05*List_B_CB)))>20  then List_B_CB-(0.05*List_B_CB)
     when (Comp_min_price - OP) between 20 and 50 then List_B_CB


     when (Comp_min_price - OP) between 50 and 75 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.15*List_B_CB)))>20  then List_B_CB-(0.15*List_B_CB)
     when (Comp_min_price - OP) between 50 and 75 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.10*List_B_CB)))>20  then List_B_CB-(0.10*List_B_CB)
     when (Comp_min_price - OP) between 50 and 75 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.05*List_B_CB)))>20  then List_B_CB-(0.05*List_B_CB)
     when (Comp_min_price - OP) between 50 and 75 then List_B_CB


     when (Comp_min_price - OP) between 75 and 100 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.20*List_B_CB)))>20  then List_B_CB-(0.20*List_B_CB)
     when (Comp_min_price - OP) between 75 and 100 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.15*List_B_CB)))>20  then List_B_CB-(0.15*List_B_CB)
     when (Comp_min_price - OP) between 75 and 100 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.10*List_B_CB)))>20  then List_B_CB-(0.10*List_B_CB)
     when (Comp_min_price - OP) between 75 and 100 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.05*List_B_CB)))>20  then List_B_CB-(0.05*List_B_CB)
     when (Comp_min_price - OP) between 75 and 100 then List_B_CB


     when (Comp_min_price - OP) between 100 and 150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.30*List_B_CB)))>20  then List_B_CB-(0.30*List_B_CB)
     when (Comp_min_price - OP) between 100 and 150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.25*List_B_CB)))>20  then List_B_CB-(0.25*List_B_CB)
     when (Comp_min_price - OP) between 100 and 150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.20*List_B_CB)))>20  then List_B_CB-(0.20*List_B_CB)
     when (Comp_min_price - OP) between 100 and 150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.15*List_B_CB)))>20  then List_B_CB-(0.15*List_B_CB)
     when (Comp_min_price - OP) between 100 and 150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.10*List_B_CB)))>20  then List_B_CB-(0.10*List_B_CB)
     when (Comp_min_price - OP) between 100 and 150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.05*List_B_CB)))>20  then List_B_CB-(0.05*List_B_CB)
     when (Comp_min_price - OP) between 100 and 150 then List_B_CB



     when (Comp_min_price - OP) >150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.40*List_B_CB)))>20  then List_B_CB-(0.40*List_B_CB)
     when (Comp_min_price - OP) >150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.35*List_B_CB)))>20  then List_B_CB-(0.35*List_B_CB)
     when (Comp_min_price - OP) >150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.30*List_B_CB)))>20  then List_B_CB-(0.30*List_B_CB)
     when (Comp_min_price - OP) >150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.25*List_B_CB)))>20  then List_B_CB-(0.25*List_B_CB)
     when (Comp_min_price - OP) >150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.20*List_B_CB)))>20  then List_B_CB-(0.20*List_B_CB)
     when (Comp_min_price - OP) >150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.15*List_B_CB)))>20  then List_B_CB-(0.15*List_B_CB)
     when (Comp_min_price - OP) >150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.10*List_B_CB)))>20  then List_B_CB-(0.10*List_B_CB)
     when (Comp_min_price - OP) >150 and s.Comp_min_price-(s.selling_price-(List_B_CB-(0.05*List_B_CB)))>20  then List_B_CB-(0.05*List_B_CB)

     when (Comp_min_price - OP) >150 then List_B_CB
     
     
     else null end as 'Final_Cashback'

from sd_comp_price s
)
;    


drop table if exists sd_comp_price_cb_final_rounding;
create local temp table sd_comp_price_cb_final_rounding on commit preserve rows as
(

select 
a.supc,
a.vendor_code,
a.product_sid,
a.vendor_sid,
Comp_min_price,
selling_price,
List_B_CB,
rounded_cashback as 'Rounded_Cashback',
(selling_price-rounded_cashback) as 'Final_price',

case when (selling_price-rounded_cashback)<79 or (selling_price-rounded_cashback)>=1999 then  rounded_cashback
     when (selling_price-rounded_cashback)= b.rounding then rounded_cashback
	 when (selling_price-rounded_cashback)= b.rounding_next then rounded_cashback
	 
     when (selling_price-rounded_cashback)< b.rounding_next and (b.rounding_next-(selling_price-rounded_cashback))<= ((selling_price-rounded_cashback)-b.rounding) and (rounded_cashback-(b.rounding_next-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_next-(selling_price-rounded_cashback)))
	 when (selling_price-rounded_cashback)< b.rounding_next and (b.rounding_next-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding) and sd_share_pre_cashback-(rounded_cashback+((selling_price-rounded_cashback)-b.rounding))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (rounded_cashback+((selling_price-rounded_cashback)-b.rounding))
	 when (selling_price-rounded_cashback)< b.rounding_next and (b.rounding_next-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding) and (rounded_cashback-(b.rounding_next-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_next-(selling_price-rounded_cashback)))
          
     when (selling_price-rounded_cashback) between b.rounding and b.rounding_1 and (b.rounding_1-(selling_price-rounded_cashback))<= ((selling_price-rounded_cashback)-b.rounding) and (rounded_cashback-(b.rounding_1-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_1-(selling_price-rounded_cashback)))
	 when (selling_price-rounded_cashback) between b.rounding and b.rounding_1 and (b.rounding_1-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding) and sd_share_pre_cashback-(rounded_cashback+((selling_price-rounded_cashback)-b.rounding))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (rounded_cashback+((selling_price-rounded_cashback)-b.rounding))
     when (selling_price-rounded_cashback) between b.rounding and b.rounding_1 and (b.rounding_1-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding) and (rounded_cashback-(b.rounding_1-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_1-(selling_price-rounded_cashback)))
     
	 when (selling_price-rounded_cashback) between b.rounding_1 and b.rounding_2 and (b.rounding_2-(selling_price-rounded_cashback))<= ((selling_price-rounded_cashback)-b.rounding_1) and (rounded_cashback-(b.rounding_2-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_2-(selling_price-rounded_cashback)))
	 when (selling_price-rounded_cashback) between b.rounding_1 and b.rounding_2 and (b.rounding_2-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding_1) and sd_share_pre_cashback-(rounded_cashback+((selling_price-rounded_cashback)-b.rounding_1))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (rounded_cashback+((selling_price-rounded_cashback)-b.rounding_1))
     when (selling_price-rounded_cashback) between b.rounding_1 and b.rounding_2 and (b.rounding_2-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding_1) and (rounded_cashback-(b.rounding_2-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_2-(selling_price-rounded_cashback)))
     
	 when (selling_price-rounded_cashback) between b.rounding_2 and b.rounding_3 and (b.rounding_3-(selling_price-rounded_cashback))<= ((selling_price-rounded_cashback)-b.rounding_2) and (rounded_cashback-(b.rounding_3-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_3-(selling_price-rounded_cashback)))
     when (selling_price-rounded_cashback) between b.rounding_2 and b.rounding_3 and (b.rounding_3-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding_2) and sd_share_pre_cashback-(rounded_cashback+((selling_price-rounded_cashback)-b.rounding_2))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (rounded_cashback+((selling_price-rounded_cashback)-b.rounding_2))
     when (selling_price-rounded_cashback) between b.rounding_2 and b.rounding_3 and (b.rounding_3-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding_2) and (rounded_cashback-(b.rounding_3-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_3-(selling_price-rounded_cashback)))
     
	 when (selling_price-rounded_cashback) between b.rounding_3 and b.rounding_4 and (b.rounding_4-(selling_price-rounded_cashback))<= ((selling_price-rounded_cashback)-b.rounding_3) and (rounded_cashback-(b.rounding_4-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_4-(selling_price-rounded_cashback)))
     when (selling_price-rounded_cashback) between b.rounding_3 and b.rounding_4 and (b.rounding_4-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding_3) and sd_share_pre_cashback-(rounded_cashback+((selling_price-rounded_cashback)-b.rounding_3))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (rounded_cashback+((selling_price-rounded_cashback)-b.rounding_3))
     when (selling_price-rounded_cashback) between b.rounding_3 and b.rounding_4 and (b.rounding_4-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding_3) and (rounded_cashback-(b.rounding_4-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_4-(selling_price-rounded_cashback)))
	 
	 when (selling_price-rounded_cashback) between b.rounding_4 and b.rounding_next and (b.rounding_next-(selling_price-rounded_cashback))<= ((selling_price-rounded_cashback)-b.rounding_4) and (rounded_cashback-(b.rounding_next-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_next-(selling_price-rounded_cashback)))
     when (selling_price-rounded_cashback) between b.rounding_4 and b.rounding_next and (b.rounding_next-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding_4) and sd_share_pre_cashback-(rounded_cashback+((selling_price-rounded_cashback)-b.rounding_4))/(case when variant1_adjusted=0 then 1 else 1.18 end)>=LEAST(variant1_gm_floor+(Slab-1)*30,variant1_gm_floor+9*30) then (rounded_cashback+((selling_price-rounded_cashback)-b.rounding_4))
     when (selling_price-rounded_cashback) between b.rounding_4 and b.rounding_next and (b.rounding_next-(selling_price-rounded_cashback))>= ((selling_price-rounded_cashback)-b.rounding_4) and (rounded_cashback-(b.rounding_next-(selling_price-rounded_cashback)))>=0 then (rounded_cashback-(b.rounding_next-(selling_price-rounded_cashback)))

     else (rounded_cashback) end as rounded_cashback_rounded

from
(
select
s.*,
case when (selling_price-Final_Cashback)<75 or (selling_price-Final_Cashback)>=1999 then  Final_Cashback
when (selling_price-Final_Cashback)= l.rounding then Final_Cashback
when (selling_price-Final_Cashback)= l.rounding_next then Final_Cashback
when l.rounding_next is not null and (selling_price-Final_Cashback) <= l.rounding_next and (Final_Cashback-(l.rounding_next-(selling_price-Final_Cashback))) > 0 then (Final_Cashback-(l.rounding_next-(selling_price-Final_Cashback)))
when l.rounding_1 is not null and (selling_price-Final_Cashback) <= l.rounding_1       and (Final_Cashback-(l.rounding_1-(selling_price-Final_Cashback))) > 0    then (Final_Cashback-(l.rounding_1-(selling_price-Final_Cashback)))
when l.rounding_2 is not null and (selling_price-Final_Cashback) <= l.rounding_2       and (Final_Cashback-(l.rounding_2-(selling_price-Final_Cashback))) > 0    then (Final_Cashback-(l.rounding_2-(selling_price-Final_Cashback)))
when l.rounding_3 is not null and (selling_price-Final_Cashback) <= l.rounding_3       and (Final_Cashback-(l.rounding_3-(selling_price-Final_Cashback))) > 0    then (Final_Cashback-(l.rounding_3-(selling_price-Final_Cashback)))
when l.rounding_4 is not null and (selling_price-Final_Cashback) <= l.rounding_4       and (Final_Cashback-(l.rounding_4-(selling_price-Final_Cashback))) > 0    then (Final_Cashback-(l.rounding_4-(selling_price-Final_Cashback)))
else (Final_Cashback) end as 'rounded_cashback',
case when Percent_decrease between 0 and 5 then '1.0_TO_5'
     when Percent_decrease between 5 and 10 then '2.5_TO_10'
     when Percent_decrease between 10 and 20 then '3.10_TO_20'
     when Percent_decrease between 20 and 30 then '4.20_TO_30'
     when Percent_decrease between 30 and 50 then '5.30_TO_50'
     when Percent_decrease >50 then '6.>50'
    else null end as 'Bucket'
from sd_comp_price_cb_final s
left join analytics_logistics.rounding_exc_2 l on (s.selling_price-s.Final_Cashback) >= l.rounding and (s.selling_price-s.Final_Cashback) <l.rounding_next
)a
left join analytics_logistics.rounding_exc_2 b on (a.selling_price-a.rounded_cashback) >= b.rounding and (a.selling_price-a.rounded_cashback) <b.rounding_next
where a.Final_Cashback<a.List_B_CB
and (a.selling_price-a.rounded_cashback)< a.Comp_min_price
);

update analytics_pricing.Cashback_list_final a
set List_H_CB=rounded_cashback_rounded
from sd_comp_price_cb_final_rounding b
where a.product_sid=b.product_sid and a.vendor_sid=b.vendor_sid;
*/

UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step8';

INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step9',(SELECT NOW()),NULL;

/*drop table if exists motz_cb ;
CREATE LOCAL TEMP TABLE motz_cb on commit preserve rows as
select distinct c.supc,c.vendor_code,c.cash_back,RO_Number,start_datetime,end_datetime,CB_add,Flag,SD_Share  from (
Select b.*, rank() over (partition by  lower(supc), lower(vendor_Code) order by start_datetime DESC) AS rank1 from
(
select a.*
from (select distinct a.*,b.start_datetime,RO_Number,SD_Share,CB_add,cash_back,end_Datetime,flag,
case when lower(a.supc)=lower(b.supc) and lower(b.vendor_code)=lower(a.vendor_code) and current_date between date(b.start_datetime) and date(b.end_datetime)
 then 'Non-SLI' else 'NA' end as 'Status' 
from analytics_pricing.Cashback_list_final a
--join analytics_pricing.monetization_supc m on lower(m.supc)=lower(f.supc) and lower(m.vendor_code)=lower(f.seller_code)
JOIN (SELECT lower(supc) as supc, lower(vendor_Code) as vendor_Code,RO_Number, cash_back, start_datetime, end_Datetime,SD_Share,include_algo_cb as CB_add,flag
	from analytics_pricing.monetization_supc  where Flag='Non-SLI' GROUP BY 1,2,3,4,5,6,7,8,9) b ON lower(a.supc) = lower(b.supc)
	and current_date between date(b.start_datetime) and date(b.end_datetime) AND lower(a.vendor_code) = lower(b.vendor_code)
	)a
where Status='Non-SLI'
)b
)c
where rank1=1 
;
*/

TRUNCATE TABLE analytics_pricing.Cashback_list_final_motz;
insert into analytics_pricing.Cashback_list_final_motz 
select supc,vendor_code,product_sid,vendor_sid,pog_id,bucket_id,subcategory_id,subcategory_name,category_id,category_name,new_supercategory,Product_name,brand_name,
brand_id,vip_selling_price,selling_price,seller_price,oneship_charges,fixed_margin_amount,fulfillment_charges,logistics_cost,closing_fee,payment_collection_charges,
reverse_logistics_charges_rto_forward,reverse_logistics_charges_rpr_forward,slab,sd_share_pre_cashback,is_enabled,is_enabled_with_inv,cashback,vip_cashback,vip_updated,
Sold_7_flag_pog,Sold_30_flag_pog,last_7_day_pog_sale,last_30_day_pog_sale,lastD7Sale,lastD15Sale,priceBucket,brand_mandate_exception,subcategory_exception,subcat_vendor_exception,
brand_exception,supc_vendor_exception,supc_exception,vendor_exception,brand_vendor_exception,is_exception,is_hard_exception,bad_seller,high_cb_pog,sale_participation,top_vendor,
category_request_cb,price_drop_vendor,variant1_name,variant1_min_gm_percent,variant1_gm_floor,variant1_nm_percent,variant1_adjusted,variant2_name,variant2_min_gm_percent,
variant2_gm_floor,variant2_nm_percent,variant2_adjusted,variant3_name,variant3_min_gm_percent,variant3_gm_floor,variant3_nm_percent,variant3_adjusted,variant4_name,
variant4_min_gm_percent,variant4_gm_floor,variant4_nm_percent,variant4_adjusted,variant5_name,variant5_min_gm_percent,variant5_gm_floor,variant5_nm_percent,variant5_adjusted,
variant6_name,variant6_min_gm_percent,variant6_gm_floor,variant6_nm_percent,variant6_adjusted,variant7_name,variant7_min_gm_percent,variant7_gm_floor,variant7_nm_percent,
variant7_adjusted,variant8_name,variant8_min_gm_percent,variant8_gm_floor,variant8_nm_percent,variant8_adjusted,price_drop_pct,reduce_sp_pct,cm_gm_movement_pct,bid,bid_active_cpt,
rto_factor,rpr_factor,cpt_income,rto_recovery,rpr_recovery,ro_monetisation,cat_motz_cb,cat_motz_CB_add,pl_bau_op,pl_bau_cb,pl_sale_op,pl_sale_cb,CategoryGroupNew,IsCore_new,
Final_ElasticityTag,Elasticity_Pct,new_logic_status,new_logic_reason,seller_zone,bau_op,sale_op,live_flag,Is_pushed,Sale_Variant,Non_Sale_Variant,base_cb,base_cb1,base_cb2,
base_cb3,base_cb4,base_cb5,sale1_cb,sale2_cb,sale3_cb,sale4_cb,base_cb_rounded,base_cb1_rounded,base_cb2_rounded,base_cb3_rounded,base_cb4_rounded,base_cb5_rounded,sale1_cb_rounded,
sale2_cb_rounded,sale3_cb_rounded,sale4_cb_rounded,cb_update_flag,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_A_CB+cat_motz_cb end as List_A_CB,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_B_CB+cat_motz_cb end as List_B_CB,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_C_CB+cat_motz_cb end as List_C_CB,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_D_CB+cat_motz_cb end as List_D_CB,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_E_CB+cat_motz_cb end as List_E_CB,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_F_CB+cat_motz_cb end as List_F_CB,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_G_CB+cat_motz_cb end as List_G_CB,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_H_CB+cat_motz_cb end as List_H_CB,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_I_CB+cat_motz_cb end as List_I_CB,
case when lower(cat_motz_cb_add)='no' and cat_motz_cb>0 then cat_motz_cb else a.List_J_CB+cat_motz_cb end as List_J_CB   
from analytics_pricing.Cashback_list_final a
;


TRUNCATE TABLE analytics_pricing.Cashback_list_final_table;
insert into analytics_pricing.Cashback_list_final_table  
select supc,vendor_code,product_sid,vendor_sid,pog_id,bucket_id,subcategory_id,subcategory_name,category_id,category_name,new_supercategory,Product_name,brand_name,
brand_id,vip_selling_price,selling_price,seller_price,oneship_charges,fixed_margin_amount,fulfillment_charges,logistics_cost,closing_fee,payment_collection_charges,
reverse_logistics_charges_rto_forward,reverse_logistics_charges_rpr_forward,slab,sd_share_pre_cashback,is_enabled,is_enabled_with_inv,cashback,vip_cashback,vip_updated,
Sold_7_flag_pog,Sold_30_flag_pog,last_7_day_pog_sale,last_30_day_pog_sale,lastD7Sale,lastD15Sale,priceBucket,brand_mandate_exception,subcategory_exception,subcat_vendor_exception,
brand_exception,supc_vendor_exception,supc_exception,vendor_exception,brand_vendor_exception,is_exception,is_hard_exception,bad_seller,high_cb_pog,sale_participation,top_vendor,
category_request_cb,price_drop_vendor,variant1_name,variant1_min_gm_percent,variant1_gm_floor,variant1_nm_percent,variant1_adjusted,variant2_name,variant2_min_gm_percent,
variant2_gm_floor,variant2_nm_percent,variant2_adjusted,variant3_name,variant3_min_gm_percent,variant3_gm_floor,variant3_nm_percent,variant3_adjusted,variant4_name,
variant4_min_gm_percent,variant4_gm_floor,variant4_nm_percent,variant4_adjusted,variant5_name,variant5_min_gm_percent,variant5_gm_floor,variant5_nm_percent,variant5_adjusted,
variant6_name,variant6_min_gm_percent,variant6_gm_floor,variant6_nm_percent,variant6_adjusted,variant7_name,variant7_min_gm_percent,variant7_gm_floor,variant7_nm_percent,
variant7_adjusted,variant8_name,variant8_min_gm_percent,variant8_gm_floor,variant8_nm_percent,variant8_adjusted,price_drop_pct,reduce_sp_pct,cm_gm_movement_pct,bid,bid_active_cpt,
rto_factor,rpr_factor,cpt_income,rto_recovery,rpr_recovery,ro_monetisation,cat_motz_cb,cat_motz_CB_add,pl_bau_op,pl_bau_cb,pl_sale_op,pl_sale_cb,CategoryGroupNew,IsCore_new,
Final_ElasticityTag,Elasticity_Pct,new_logic_status,new_logic_reason,seller_zone,bau_op,sale_op,live_flag,Is_pushed,Sale_Variant,Non_Sale_Variant,base_cb,base_cb1,base_cb2,
base_cb3,base_cb4,base_cb5,sale1_cb,sale2_cb,sale3_cb,sale4_cb,base_cb_rounded,base_cb1_rounded,base_cb2_rounded,base_cb3_rounded,base_cb4_rounded,base_cb5_rounded,sale1_cb_rounded,
sale2_cb_rounded,sale3_cb_rounded,sale4_cb_rounded,cb_update_flag,
List_A_CB,
case when pl_bau_op>0 then least((case when pl_bau_op>=a.selling_price then 0 when pl_bau_op<=a.selling_price then a.selling_price-pl_bau_op else pl_bau_cb end),pl_bau_cb) else List_B_CB end as List_B_CB,
case when pl_sale_op>0 then least((case when pl_sale_op>=a.selling_price then 0 when pl_sale_op<=a.selling_price then a.selling_price-pl_sale_op else pl_sale_cb end),pl_sale_cb) else List_C_CB end as List_C_CB,
case when pl_sale_op>0 then least((case when pl_sale_op>=a.selling_price then 0 when pl_sale_op<=a.selling_price then a.selling_price-pl_sale_op else pl_sale_cb end),pl_sale_cb) else List_D_CB end as List_D_CB,
case when pl_bau_op>0 then least((case when pl_bau_op>=a.selling_price then 0 when pl_bau_op<=a.selling_price then a.selling_price-pl_bau_op else pl_bau_cb end),pl_bau_cb) else List_E_CB end as List_E_CB,
case when pl_sale_op>0 then least((case when pl_sale_op>=a.selling_price then 0 when pl_sale_op<=a.selling_price then a.selling_price-pl_sale_op else pl_sale_cb end),pl_sale_cb) else List_F_CB end as List_F_CB,
List_G_CB,
List_H_CB,
case when pl_bau_op>0 then least((case when pl_bau_op>=a.selling_price then 0 when pl_bau_op<=a.selling_price then a.selling_price-pl_bau_op else pl_bau_cb end),pl_bau_cb) else List_I_CB end as List_I_CB,
case when pl_sale_op>0 then least((case when pl_sale_op>=a.selling_price then 0 when pl_sale_op<=a.selling_price then a.selling_price-pl_sale_op else pl_sale_cb end),pl_sale_cb) else List_J_CB end as List_J_CB   
from analytics_pricing.Cashback_list_final_motz a
;

/*update analytics_pricing.Cashback_list_final a
set List_B_CB=case when pl_bau_op>0 then least((case when pl_bau_op>=a.selling_price then 0
                                                     when pl_bau_op<=a.selling_price then a.selling_price-pl_bau_op
                                                     else pl_bau_cb end),pl_bau_cb) else List_B_CB end ,
    List_E_CB=case when pl_bau_op>0 then least((case when pl_bau_op>=a.selling_price then 0
                                                     when pl_bau_op<=a.selling_price then a.selling_price-pl_bau_op
                                                     else pl_bau_cb end),pl_bau_cb) else List_E_CB end ,
    List_I_CB=case when pl_bau_op>0 then least((case when pl_bau_op>=a.selling_price then 0
                                                     when pl_bau_op<=a.selling_price then a.selling_price-pl_bau_op
                                                     else pl_bau_cb end),pl_bau_cb) else List_I_CB end 						  
;

update analytics_pricing.Cashback_list_final a
set List_C_CB= case when pl_sale_op>0 then least((case when pl_sale_op>=a.selling_price then 0
                                                       when pl_sale_op<=a.selling_price then a.selling_price-pl_sale_op
                                                       else pl_sale_cb end),pl_sale_cb) else List_C_CB end,
    List_F_CB= case when pl_sale_op>0 then least((case when pl_sale_op>=a.selling_price then 0
                                                       when pl_sale_op<=a.selling_price then a.selling_price-pl_sale_op
                                                       else pl_sale_cb end),pl_sale_cb) else List_F_CB end,
    List_J_CB= case when pl_sale_op>0 then least((case when pl_sale_op>=a.selling_price then 0
                                                       when pl_sale_op<=a.selling_price then a.selling_price-pl_sale_op
                                                       else pl_sale_cb end),pl_sale_cb) else List_J_CB end    						  
;*/

UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step9';

INSERT INTO analytics_pricing.DataupdateSteps
SELECT 'Step10',(SELECT NOW()),NULL;



TRUNCATE TABLE analytics_pricing.cash_back_final_update_supc_vendorcode;
insert into analytics_pricing.cash_back_final_update_supc_vendorcode 
SELECT 
*,
0 as vipCashbackA,0 as vipCashbackB,0 as vipCashbackC,0 as vipCashbackD,0 as vipCashbackE,0 as vipCashbackF,0 as vipCashbackG,0 as vipCashbackH,
/*case when brand_id in ('555111', '227283', '447758', '437369', '563564', '285432', '468438', '448358', '564053', '556592') then 0 else */
greatest(List_C_CB-List_B_CB,0) as vipCashbackI,
/*case when brand_id in ('555111', '227283', '447758', '437369', '563564', '285432', '468438', '448358', '564053', '556592') then 0 else*/ 
greatest(List_D_CB-List_C_CB,0) as vipCashbackJ,
0 as isRevenueRecognised,
'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeA,'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeB,
'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeC,'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeD,
'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeE,'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeF,
'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeG,'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeH,
'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeI,'SD'||'_'||'DISC'||'_'||category_id||'CATS'||'_'||'0' as rrCouponCodeJ,
0 as additionalCashbackA,0 as additionalCashbackB,0 as additionalCashbackC,0 as additionalCashbackD,0 as additionalCashbackE,0 as additionalCashbackF,
0 as additionalCashbackG,0 as additionalCashbackH,0 as additionalCashbackI,0 as additionalCashbackJ

FROM ( 
select distinct current_date as date,
supc,vendor_code,a.product_sid,a.vendor_sid,pog_id,bucket_id,subcategory_id,category_id,
brand_id,vip_selling_price,selling_price,seller_price,oneship_charges,fixed_margin_amount,fulfillment_charges,logistics_cost,closing_fee,payment_collection_charges,
reverse_logistics_charges_rto_forward,reverse_logistics_charges_rpr_forward,slab,sd_share_pre_cashback,is_enabled,is_enabled_with_inv,cashback,vip_cashback,vip_updated,
Sold_7_flag_pog,Sold_30_flag_pog,last_7_day_pog_sale,last_30_day_pog_sale,lastD7Sale,lastD15Sale,priceBucket,brand_mandate_exception,subcategory_exception,
subcat_vendor_exception,brand_exception,supc_vendor_exception,supc_exception,vendor_exception,brand_vendor_exception,is_exception,is_hard_exception,bad_seller,high_cb_pog,sale_participation,
top_vendor,category_request_cb,price_drop_vendor,variant1_name,variant1_min_gm_percent,variant1_gm_floor,variant1_nm_percent,variant1_adjusted,variant2_name,variant2_min_gm_percent,variant2_gm_floor,variant2_nm_percent,variant2_adjusted,
variant3_name,variant3_min_gm_percent,variant3_gm_floor,variant3_nm_percent,variant3_adjusted,variant4_name,variant4_min_gm_percent,variant4_gm_floor,variant4_nm_percent,
variant4_adjusted,variant5_name,variant5_min_gm_percent,variant5_gm_floor,variant5_nm_percent,variant5_adjusted,variant6_name,variant6_min_gm_percent,variant6_gm_floor,variant6_nm_percent,variant6_adjusted,
variant7_name,variant7_min_gm_percent,variant7_gm_floor,variant7_nm_percent,variant7_adjusted,variant8_name,variant8_min_gm_percent,variant8_gm_floor,variant8_nm_percent,variant8_adjusted,
price_drop_pct,reduce_sp_pct,cm_gm_movement_pct,bid,bid_active_cpt,rto_factor,rpr_factor,cpt_income,rto_recovery,rpr_recovery,ro_monetisation,cat_motz_cb,cat_motz_CB_add,pl_bau_op,pl_bau_cb,pl_sale_op,pl_sale_cb,CategoryGroupNew,IsCore_new,Final_ElasticityTag,Elasticity_Pct,
new_logic_status,new_logic_reason,seller_zone,
bau_op,sale_op,live_flag,Is_pushed,cb_update_flag,Sale_Variant,Non_Sale_Variant,base_cb,base_cb1,base_cb2,base_cb3,base_cb4,base_cb5,sale1_cb,sale2_cb,sale3_cb,sale4_cb,base_cb_rounded,
base_cb1_rounded,base_cb2_rounded,base_cb3_rounded,base_cb4_rounded,base_cb5_rounded,sale1_cb_rounded,sale2_cb_rounded,sale3_cb_rounded,sale4_cb_rounded,	

Round(case when List_B_CB<0 then 0 
           when a.vip_selling_price-a.List_B_CB>bau_op then a.List_B_CB
           when a.vip_selling_price-a.List_B_CB<bau_op and a.vip_selling_price<bau_op then 0
           when a.vip_selling_price-a.List_B_CB<bau_op and a.vip_selling_price=bau_op then 0
           when a.vip_selling_price-a.List_B_CB<bau_op then a.vip_selling_price-bau_op
           when a.vip_selling_price-a.List_B_CB=bau_op then a.List_B_CB
           else a.List_B_CB end,2) as List_A_CB,
Round(case when List_B_CB<0 then 0 
           when a.vip_selling_price-a.List_B_CB>bau_op then a.List_B_CB
           when a.vip_selling_price-a.List_B_CB<bau_op and a.vip_selling_price<bau_op then 0
           when a.vip_selling_price-a.List_B_CB<bau_op and a.vip_selling_price=bau_op then 0
           when a.vip_selling_price-a.List_B_CB<bau_op then a.vip_selling_price-bau_op
           when a.vip_selling_price-a.List_B_CB=bau_op then a.List_B_CB
           else a.List_B_CB end,2) as List_B_CB,
Round(case when List_C_CB<0 then 0 
           when a.vip_selling_price-a.List_C_CB>sale_op then a.List_C_CB
           when a.vip_selling_price-a.List_C_CB<sale_op and a.vip_selling_price<sale_op then 0
           when a.vip_selling_price-a.List_C_CB<sale_op and a.vip_selling_price=sale_op then 0
           when a.vip_selling_price-a.List_C_CB<sale_op then a.vip_selling_price-sale_op
           when a.vip_selling_price-a.List_C_CB=sale_op then a.List_C_CB
           else a.List_C_CB end,2) as List_C_CB,
Round(case when List_D_CB<0 then 0 
           when a.vip_selling_price-a.List_D_CB>sale_op then a.List_D_CB
           when a.vip_selling_price-a.List_D_CB<sale_op and a.vip_selling_price<sale_op then 0
           when a.vip_selling_price-a.List_D_CB<sale_op and a.vip_selling_price=sale_op then 0
           when a.vip_selling_price-a.List_D_CB<sale_op then a.vip_selling_price-sale_op
           when a.vip_selling_price-a.List_D_CB=sale_op then a.List_D_CB
           else a.List_D_CB end,2) as List_D_CB,
Round(case when List_E_CB<0 then 0 
           when a.vip_selling_price-a.List_E_CB>bau_op then a.List_E_CB
           when a.vip_selling_price-a.List_E_CB<bau_op and a.vip_selling_price<bau_op then 0
           when a.vip_selling_price-a.List_E_CB<bau_op and a.vip_selling_price=bau_op then 0
           when a.vip_selling_price-a.List_E_CB<bau_op then a.vip_selling_price-bau_op
           when a.vip_selling_price-a.List_E_CB=bau_op then a.List_E_CB
           else a.List_E_CB end,2) as List_E_CB,
Round(case when List_F_CB<0 then 0 
           when a.vip_selling_price-a.List_F_CB>sale_op then a.List_F_CB
           when a.vip_selling_price-a.List_F_CB<sale_op and a.vip_selling_price<sale_op then 0
           when a.vip_selling_price-a.List_F_CB<sale_op and a.vip_selling_price=sale_op then 0
           when a.vip_selling_price-a.List_F_CB<sale_op then a.vip_selling_price-sale_op
           when a.vip_selling_price-a.List_F_CB=sale_op then a.List_F_CB
           else a.List_F_CB end,2) as List_F_CB,	
Round(case when List_G_CB<0 then 0 
           when a.vip_selling_price-a.List_G_CB>bau_op then a.List_G_CB
           when a.vip_selling_price-a.List_G_CB<bau_op and a.vip_selling_price<bau_op then 0
           when a.vip_selling_price-a.List_G_CB<bau_op and a.vip_selling_price=bau_op then 0
           when a.vip_selling_price-a.List_G_CB<bau_op then a.vip_selling_price-bau_op
           when a.vip_selling_price-a.List_G_CB=bau_op then a.List_G_CB
           else a.List_G_CB end,2) as List_G_CB,
Round(case when List_H_CB<0 then 0 
           when a.vip_selling_price-a.List_H_CB>bau_op then a.List_H_CB
           when a.vip_selling_price-a.List_H_CB<bau_op and a.vip_selling_price<bau_op then 0
           when a.vip_selling_price-a.List_H_CB<bau_op and a.vip_selling_price=bau_op then 0
           when a.vip_selling_price-a.List_H_CB<bau_op then a.vip_selling_price-bau_op
           when a.vip_selling_price-a.List_H_CB=bau_op then a.List_H_CB
           else a.List_H_CB end,2) as List_H_CB,
Round(case when List_I_CB<0 then 0 
           when a.vip_selling_price-a.List_I_CB>bau_op then a.List_I_CB
           when a.vip_selling_price-a.List_I_CB<bau_op and a.vip_selling_price<bau_op then 0
           when a.vip_selling_price-a.List_I_CB<bau_op and a.vip_selling_price=bau_op then 0
           when a.vip_selling_price-a.List_I_CB<bau_op then a.vip_selling_price-bau_op
           when a.vip_selling_price-a.List_I_CB=bau_op then a.List_I_CB
           else a.List_I_CB end,2) as List_I_CB,	
Round(case when List_J_CB<0 then 0 
           when a.vip_selling_price-a.List_J_CB>sale_op then a.List_J_CB
           when a.vip_selling_price-a.List_J_CB<sale_op and a.vip_selling_price<sale_op then 0
           when a.vip_selling_price-a.List_J_CB<sale_op and a.vip_selling_price=sale_op then 0
           when a.vip_selling_price-a.List_J_CB<sale_op then a.vip_selling_price-sale_op
           when a.vip_selling_price-a.List_J_CB=sale_op then a.List_J_CB
           else a.List_J_CB end,2) as List_J_CB
                  
from 
analytics_pricing.Cashback_list_final_table a
)a
--where a.supc in ('SDL801239733') --and-- a.vendor_code in ('Sf59a4','240079')  
;


DROP TABLE IF EXISTS price_match_product;
CREATE LOCAL TEMP TABLE price_match_product on commit preserve rows as
select *,
case when NM<0 then new_cb+NM*1.18 else new_cb end as new_cb_final,
SD_Share_pre_CB- (case when NM<0 then new_cb+NM*1.18 else new_cb end)/1.18 as New_SD_share_post_cb_0_NM,
round((SD_Share_pre_CB- (case when NM<0 then new_cb+NM*1.18 else new_cb end)/1.18)-ffc,2) as new_nm
from
(
select a.supc,a.vendor_code,a.brand_name,a.brand_id,az_sp,fk_sp,least(az_sp,fk_sp) as min_sp,selling_price,seller_price,
list_B_CB,selling_price-list_B_CB as op,(selling_price-seller_price)/1.18 as 'SD_Share_pre_CB',
((selling_price-seller_price)/1.18)-list_B_CB/1.18 as SD_Share_post_CB,
(case when least(az_sp,fk_sp)<selling_price-list_B_CB then (selling_price-list_B_CB)-least(az_sp,fk_sp) else 0 end)+list_B_CB as new_cb,
((selling_price-seller_price)/1.18)-((case when least(az_sp,fk_sp)<selling_price-list_B_CB then (selling_price-list_B_CB)-least(az_sp,fk_sp) else 0 end)+list_B_CB)/1.18 as New_SD_share_post_cb,
slab,
greatest((60+least((slab-1),9)*30),0) as ffc,
(((selling_price-seller_price)/1.18)-((case when least(az_sp,fk_sp)<selling_price-list_B_CB then (selling_price-list_B_CB)-least(az_sp,fk_sp) else 0 end)+list_B_CB)/1.18)-(greatest((60+least((slab-1),9)*30),0)) as NM
from analytics_pricing.price_match_product_list a
join analytics_pricing.cashback_list_final b on lower(a.supc)=lower(b.supc) and lower(a.vendor_code)=lower(b.vendor_code)
where is_hard_exception=0
--where a.supc in ('SDL664262728')
group by 1,2,3,4,5,6,8,9,10,16)a;

update analytics_pricing.cash_back_final_update_supc_vendorcode a
set is_pushed=1,
    list_B_CB=case when new_cb_final<0 then 0 else new_cb_final end
from price_match_product b   
where lower(a.supc)=lower(b.supc) and lower(a.vendor_code)=lower(b.vendor_code);

update analytics_pricing.cash_back_final_update_supc_vendorcode
set is_pushed=1
where  is_pushed=0 and list_B_CB>0 and is_enabled_with_inv=1 and is_hard_exception=0
and pog_id in (select distinct pog_id from 
(
select pog_id,sum(visits) as visits from snapdeal_reporting.pog_level_visits 
where date>=current_Date-2 and pog_id is not null
group by 1 
order by 2 desc 
)a);
	 
update analytics_pricing.cash_back_final_update_supc_vendorcode
set is_pushed=0
where variant1_name is null
;


/*insert into analytics_pricing.dod_pricing 
select supc,vendor_code,
329 as deal_op,
current_timestamp+2 as start_date,
(current_timestamp+2)+1/3.3 as end_date,
current_timestamp as created,current_timestamp as updated, 
'meetu.sondhi01@snapdeal.com' as updated_by
from analytics_pricing.cash_back_final_update_supc_vendorcode where pog_id in ('682459644912') */

update analytics_pricing.cash_back_final_update_supc_vendorcode a
set List_B_CB=case when deal_op is null then List_C_CB else selling_price-deal_op end,
    List_C_CB=case when deal_op is null then List_C_CB else selling_price-deal_op end,
    is_pushed=1
from analytics_pricing.dod_pricing b
where current_date between date(start_date) and date(end_date) and a.supc=b.supc and a.vendor_code=b.vendor_code;


/*update analytics_pricing.cash_back_final_update_supc_vendorcode a
set List_C_CB=b.List_C_CB
from analytics_pricing.pl_product_sale_bau_cb b
where lower(a.supc)=lower(b.supc) and lower(a.vendor_code)=lower(b.vendor_code)
;

update analytics_pricing.cash_back_final_update_supc_vendorcode a
set List_B_CB=b.CB
from analytics_pricing.pl_product_bau_cb b
where lower(a.supc)=lower(b.supc) and lower(a.vendor_code)=lower(b.vendor_code)
;
*/

--update analytics_pricing.cash_back_final_update_supc_vendorcode set List_B_CB=41,List_I_CB=41
--where supc||vendor_code in ('SDL441958140Sa9cf1', 'SDL716278892Sa9cf1', 'SDL713383960Sa9cf1', 'SDL586415256Sa9cf1', 'SDL782415552Sa9cf1', 'SDL784302191Sa9cf1', 'SDL737971970Sa9cf1', 'SDL428116045Sa9cf1', 'SDL584571879Sa9cf1', 'SDL550330722Sa9cf1', 'SDL508931714Sa9cf1', 'SDL805898095Sa9cf1', 'SDL822693669Sa9cf1', 'SDL838170399Sa9cf1', 'SDL548347623Sa9cf1', 'SDL544773102Sa9cf1', 'SDL584909456Sa9cf1', 'SDL817564991Sa9cf1') 


/*update analytics_pricing.cash_back_final_update_supc_vendorcode a
set List_A_CB=0,    List_B_CB=0,   List_D_CB=0, List_F_CB=0,   
    List_H_CB=0,     List_I_CB=0,    List_J_CB=0
from (
select distinct  product_sid,vendor_sid
from dwh.f_vendor_inventory_pricing fvip 
where fvip.enabled = 1
and IFNULL(fvip.signature_present,0) = 1
and IFNULL(fvip.gst_enabled,0) = 1
and fvip.block_inventory_enabled = 1
and fvip.enabled_by_seller = 1
and IFNULL(fvip.sales_velocity_reached,0) = 1
and fvip.inventory - fvip.inventory_sold > 0
group by 1,2) b
where a.product_sid=b.product_sid and a.vendor_sid=b.vendor_sid
;
*/

--and is_hard_exception=0 and is_pushed=0 and is_enabled_with_inv=1 and Sold_30_flag_pog=0;
--create table analytics_pricing.cash_back_final_update_supc_vendorcode_backup_backup as 
--select * from analytics_pricing.cash_back_final_update_supc_vendorcode_backup where is_pushed=1

DELETE FROM analytics_pricing.cash_back_final_update_supc_vendorcode_backup where snapshot_date= current_date;

insert into analytics_pricing.cash_back_final_update_supc_vendorcode_backup 
select current_date as snapshot_date,* from analytics_pricing.cash_back_final_update_supc_vendorcode
;

DELETE FROM analytics_pricing.cash_back_final_update_supc_vendorcode_backup where snapshot_date= current_date-7;

UPDATE analytics_pricing.DataupdateSteps
SET EndTime=(SELECT NOW())
WHERE StepName='Step10';


drop table if exists sale_check_D1;
create local temp table sale_check_D1 on commit preserve rows as 
(
        select fso.product_sid, fso.vendor_sid, count(distinct fso.suborder_code) as Subos_D1
        from dwh.f_suborders_oms_vw fso
        where to_date(fso.subo_date_verified::varchar,'YYYYMMDD') between '2022-07-15' and '2022-07-18' 
        group by 1,2
);

select 
case when fc.vendor_sid is not null then 1 else 0 end as Cover_Flag,
/*case when selling_price between 0 and 200 then '0-200'
     when selling_price between 201 and 999 then '201-999'
     when selling_price >=1000 then '>=1000' end as 'flag',
*/
--is_exception,is_hard_exception,bad_seller,sale_participation,Sale_Variant,
sum(Subos_D1) as Subos_D1,
sum(ifnull(List_A_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_A_CB_new,
sum(ifnull(List_B_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_B_CB_new,
sum(ifnull(List_C_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_C_CB_new,
sum(ifnull(List_D_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_D_CB_new,

sum(ifnull(List_E_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_E_CB_new,
sum(ifnull(List_F_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_F_CB_new,
sum(ifnull(List_G_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_G_CB_new,
sum(ifnull(List_H_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_H_CB_new,
sum(ifnull(List_I_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_I_CB_new,
sum(ifnull(List_J_CB,0)*ifnull(Subos_D1,0))/sum(Subos_D1) as List_J_CB_new,

count(distinct sc.vendor_sid||'-'||sc.product_sid) as seller_supc_count
from sale_check_D1 sc
join dwh.f_vendor_inventory_pricing b on sc.vendor_sid=b.vendor_sid and sc.product_sid=b.product_sid
left join (select * from analytics_pricing.cash_back_final_update_supc_vendorcode /*where is_pushed=1*/) fc on sc.vendor_sid =fc.vendor_sid and sc.product_sid =fc.product_sid
group by 1
order by 1;

select count(supc||vendor_code) ,count(distinct supc||vendor_code)  from analytics_pricing.cash_back_final_update_supc_vendorcode where is_pushed=1;

SELECT
    StepName,
    StartTime,
    EndTime,
    minute(EndTime-StartTime)         AS time_taken,
    hour(CURRENT_TIMESTAMP-StartTime) AS time_taken
FROM
    analytics_pricing.DataupdateSteps
GROUP BY
    1,2,3
ORDER BY
    2 DESC limit 200;