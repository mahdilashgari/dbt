with
stage_datafeeds_raw as
(
    select * from {{ ref('stg_webtracking__datafeeds_raw') }}
),
stage_datafeeds_mobile_attributes as
(
    select * from {{ ref('stg_webtracking__datafeeds_mobile_attributes') }}
)
--Profile Edit
select
	stage_datafeeds_raw.ACTIVITY_DATE,
	stage_datafeeds_raw.DATE_TIME as ACTIVITY_DATETIME,
	stage_datafeeds_raw.POST_VISID_HIGH || stage_datafeeds_raw.POST_VISID_LOW || stage_datafeeds_raw.VISIT_NUM || stage_datafeeds_raw.VISIT_START_TIME_GMT || stage_datafeeds_raw.VISIT_PAGE_NUM as ACTIVITY_CODE,
	stage_datafeeds_raw.POST_VISID_HIGH || stage_datafeeds_raw.POST_VISID_LOW || stage_datafeeds_raw.VISIT_NUM || stage_datafeeds_raw.VISIT_START_TIME_GMT as VISIT_CODE,
	lu.USER_ID as USER_ID,
	stage_datafeeds_raw.POST_PROP1 as APP_NAME,
	coalesce(trim(stage_datafeeds_mobile_attributes.MOBILE_DEVICE_NAME),trim(stage_datafeeds_raw.POST_MOBILEDEVICE),'Unknown') as DEVICE_NAME,
	'Profile Edit' as INTERACTION_TYPE_NAME,
	--coalesce(vt.ACTIVITY_VISIT_TYPE_ID, 0) as ACTIVITY_VISIT_TYPE_ID,
	--coalesce(ltp.TRACKING_PLATFORM_ID, 0) as TRACKING_PLATFORM_ID,
	nvl(lu.COUNTRY_BUSINESS_ID, 0) as BUSINESS_COUNTRY_ID,
	nvl(lu.COUNTRY_PRIVATE_ID, 0) as PRIVATE_COUNTRY_ID
	--coalesce(geo.COUNTRY_ID, 0) as GEO_COUNTRY_ID
from
	stage_datafeeds_raw
	left join RAW.PUBLIC.LU_USER lu on stage_datafeeds_raw.POST_EVAR3 = cast(LEFT(MD5(lu.USER_ID||'sahuVoh5'), 16) as VARCHAR(32))
	left join stage_datafeeds_mobile_attributes on stage_datafeeds_raw.MOBILE_ID = stage_datafeeds_mobile_attributes.MOBILE_ID
    /*
    left join #SEED#.LU_ACTIVITY_VISIT_TYPE vt on vt.ACTIVITY_VISIT_TYPE_NAME = case stage_datafeeds_raw.POST_PROP16 when 'logged_in' then 'Logged In' when 'soft_logged_in' then 'Soft Logged In' when 'logged_out' then 'Logged Out' end
    left join #SEED#.LU_TRACKING_PLATFORM ltp on ltp.TRACKING_PLATFORM_NAME =
                        case
                            when stage_datafeeds_raw.POST_PROP1 = 'wbm' and stage_datafeeds_mobile_attributes.MOBILE_DEVICE_TYPE = 'Mobile Phone' then 'Web - Small Screen'
                            when stage_datafeeds_raw.POST_PROP1 = 'wbm' then 'Web - Big Screen'
                            when stage_datafeeds_raw.POST_PROP1 in ('w10m','wbm_w10m') then 'Web - Big Screen'
                            when stage_datafeeds_raw.POST_PROP1 in ('iosm','wbm_iosm') then 'iOS'
                            when stage_datafeeds_raw.POST_PROP1 in ('andm','wbm_andm') then 'Android'
                        end
	left join #SEED#.LU_COUNTRY geo on lower(stage_datafeeds_raw.GEO_COUNTRY) = lower(geo.COUNTRY_CODE_ISO_3)
    */
where
	to_date(stage_datafeeds_raw.DATE_TIME) = to_date('20220505','YYYYMMDD') and
    position('20132', stage_datafeeds_raw.POST_EVENT_LIST) > 0
limit 100