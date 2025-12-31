--Profile Edit
select
	to_date(df.DATE_TIME) as ACTIVITY_DATE,
	df.DATE_TIME as ACTIVITY_DATETIME,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT || df.VISIT_PAGE_NUM as ACTIVITY_CODE,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT as VISIT_CODE,
	lu.USER_ID as USER_ID,
	df.POST_PROP1 as APP_NAME,
	coalesce(trim(mda.MOBILE_DEVICE_NAME),trim(df.POST_MOBILEDEVICE),'Unknown') as DEVICE_NAME,
	'Profile Edit' as INTERACTION_TYPE_NAME,
	--coalesce(vt.ACTIVITY_VISIT_TYPE_ID, 0) as ACTIVITY_VISIT_TYPE_ID,
	--coalesce(ltp.TRACKING_PLATFORM_ID, 0) as TRACKING_PLATFORM_ID,
	nvl(lu.COUNTRY_BUSINESS_ID, 0) as BUSINESS_COUNTRY_ID,
	nvl(lu.COUNTRY_PRIVATE_ID, 0) as PRIVATE_COUNTRY_ID
	--coalesce(geo.COUNTRY_ID, 0) as GEO_COUNTRY_ID
from
	RAW.PUBLIC.STAGE_DATAFEEDS_RAW df
	left join RAW.PUBLIC.LU_USER lu on df.POST_EVAR3 = cast(LEFT(MD5(lu.USER_ID||'sahuVoh5'), 16) as VARCHAR(32))
	left join RAW.PUBLIC.STAGE_DATAFEEDS_MOBILE_ATTRIBUTES mda on df.MOBILE_ID = mda.MOBILE_ID
    /*
    left join #SEED#.LU_ACTIVITY_VISIT_TYPE vt on vt.ACTIVITY_VISIT_TYPE_NAME = case df.POST_PROP16 when 'logged_in' then 'Logged In' when 'soft_logged_in' then 'Soft Logged In' when 'logged_out' then 'Logged Out' end
    left join #SEED#.LU_TRACKING_PLATFORM ltp on ltp.TRACKING_PLATFORM_NAME =
                        case
                            when df.POST_PROP1 = 'wbm' and mda.MOBILE_DEVICE_TYPE = 'Mobile Phone' then 'Web - Small Screen'
                            when df.POST_PROP1 = 'wbm' then 'Web - Big Screen'
                            when df.POST_PROP1 in ('w10m','wbm_w10m') then 'Web - Big Screen'
                            when df.POST_PROP1 in ('iosm','wbm_iosm') then 'iOS'
                            when df.POST_PROP1 in ('andm','wbm_andm') then 'Android'
                        end
	left join #SEED#.LU_COUNTRY geo on lower(df.GEO_COUNTRY) = lower(geo.COUNTRY_CODE_ISO_3)
    */
where
	to_date(df.DATE_TIME) = to_date('20220505','YYYYMMDD') and
    position('20132', df.POST_EVENT_LIST) > 0
limit 100;



--Job Click
select
	to_date(df.DATE_TIME) as ACTIVITY_DATE,
	df.DATE_TIME as ACTIVITY_DATETIME,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT || df.VISIT_PAGE_NUM as ACTIVITY_CODE,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT as VISIT_CODE,
	lu.USER_ID as USER_ID,
	df.POST_PROP1 as APP_NAME,
	coalesce(trim(mda.MOBILE_DEVICE_NAME),trim(df.POST_MOBILEDEVICE),'Unknown') as DEVICE_NAME,
	'Job Click' as INTERACTION_TYPE_NAME,
	--coalesce(vt.ACTIVITY_VISIT_TYPE_ID, 0) as ACTIVITY_VISIT_TYPE_ID,
	--coalesce(ltp.TRACKING_PLATFORM_ID, 0) as TRACKING_PLATFORM_ID,
	nvl(lu.COUNTRY_BUSINESS_ID, 0) as BUSINESS_COUNTRY_ID,
	nvl(lu.COUNTRY_PRIVATE_ID, 0) as PRIVATE_COUNTRY_ID
	--coalesce(geo.COUNTRY_ID, 0) as GEO_COUNTRY_ID
from
	RAW.PUBLIC.STAGE_DATAFEEDS_RAW df
	left join RAW.PUBLIC.LU_USER lu on df.POST_EVAR3 = cast(LEFT(MD5(lu.USER_ID||'sahuVoh5'), 16) as VARCHAR(32))
	left join RAW.PUBLIC.STAGE_DATAFEEDS_MOBILE_ATTRIBUTES mda on df.MOBILE_ID = mda.MOBILE_ID
    /*
    left join #SEED#.LU_ACTIVITY_VISIT_TYPE vt on vt.ACTIVITY_VISIT_TYPE_NAME = case df.POST_PROP16 when 'logged_in' then 'Logged In' when 'soft_logged_in' then 'Soft Logged In' when 'logged_out' then 'Logged Out' end
    left join #SEED#.LU_TRACKING_PLATFORM ltp on ltp.TRACKING_PLATFORM_NAME =
                        case
                            when df.POST_PROP1 = 'wbm' and mda.MOBILE_DEVICE_TYPE = 'Mobile Phone' then 'Web - Small Screen'
                            when df.POST_PROP1 = 'wbm' then 'Web - Big Screen'
                            when df.POST_PROP1 in ('w10m','wbm_w10m') then 'Web - Big Screen'
                            when df.POST_PROP1 in ('iosm','wbm_iosm') then 'iOS'
                            when df.POST_PROP1 in ('andm','wbm_andm') then 'Android'
                        end
	left join #SEED#.LU_COUNTRY geo on lower(df.GEO_COUNTRY) = lower(geo.COUNTRY_CODE_ISO_3)
    */
where
	to_date(df.DATE_TIME) = to_date('20220505','YYYYMMDD') and
    position('224', df.POST_EVENT_LIST) > 0
limit 100;



--Job Apply Click
select
	to_date(df.DATE_TIME) as ACTIVITY_DATE,
	df.DATE_TIME as ACTIVITY_DATETIME,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT || df.VISIT_PAGE_NUM as ACTIVITY_CODE,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT as VISIT_CODE,
	lu.USER_ID as USER_ID,
	df.POST_PROP1 as APP_NAME,
	coalesce(trim(mda.MOBILE_DEVICE_NAME),trim(df.POST_MOBILEDEVICE),'Unknown') as DEVICE_NAME,
	'Job Apply Click' as INTERACTION_TYPE_NAME,
	--coalesce(vt.ACTIVITY_VISIT_TYPE_ID, 0) as ACTIVITY_VISIT_TYPE_ID,
	--coalesce(ltp.TRACKING_PLATFORM_ID, 0) as TRACKING_PLATFORM_ID,
	nvl(lu.COUNTRY_BUSINESS_ID, 0) as BUSINESS_COUNTRY_ID,
	nvl(lu.COUNTRY_PRIVATE_ID, 0) as PRIVATE_COUNTRY_ID
	--coalesce(geo.COUNTRY_ID, 0) as GEO_COUNTRY_ID
from
	RAW.PUBLIC.STAGE_DATAFEEDS_RAW df
	left join RAW.PUBLIC.LU_USER lu on df.POST_EVAR3 = cast(LEFT(MD5(lu.USER_ID||'sahuVoh5'), 16) as VARCHAR(32))
	left join RAW.PUBLIC.STAGE_DATAFEEDS_MOBILE_ATTRIBUTES mda on df.MOBILE_ID = mda.MOBILE_ID
    /*
    left join #SEED#.LU_ACTIVITY_VISIT_TYPE vt on vt.ACTIVITY_VISIT_TYPE_NAME = case df.POST_PROP16 when 'logged_in' then 'Logged In' when 'soft_logged_in' then 'Soft Logged In' when 'logged_out' then 'Logged Out' end
    left join #SEED#.LU_TRACKING_PLATFORM ltp on ltp.TRACKING_PLATFORM_NAME =
                        case
                            when df.POST_PROP1 = 'wbm' and mda.MOBILE_DEVICE_TYPE = 'Mobile Phone' then 'Web - Small Screen'
                            when df.POST_PROP1 = 'wbm' then 'Web - Big Screen'
                            when df.POST_PROP1 in ('w10m','wbm_w10m') then 'Web - Big Screen'
                            when df.POST_PROP1 in ('iosm','wbm_iosm') then 'iOS'
                            when df.POST_PROP1 in ('andm','wbm_andm') then 'Android'
                        end
	left join #SEED#.LU_COUNTRY geo on lower(df.GEO_COUNTRY) = lower(geo.COUNTRY_CODE_ISO_3)
    */
where
	to_date(df.DATE_TIME) = to_date('20220505','YYYYMMDD') and
	(
		df.POST_PROP64 like 'jobs_click_application%'
		or
		df.POST_PROP64 like '%jobs_apply_start%'
	)
limit 100;



--Contact Request Sent
select
	to_date(df.DATE_TIME) as ACTIVITY_DATE,
	df.DATE_TIME as ACTIVITY_DATETIME,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT || df.VISIT_PAGE_NUM as ACTIVITY_CODE,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT as VISIT_CODE,
	lu.USER_ID as USER_ID,
	df.POST_PROP1 as APP_NAME,
	coalesce(trim(mda.MOBILE_DEVICE_NAME),trim(df.POST_MOBILEDEVICE),'Unknown') as DEVICE_NAME,
	'Contact Request Sent' as INTERACTION_TYPE_NAME,
	--coalesce(vt.ACTIVITY_VISIT_TYPE_ID, 0) as ACTIVITY_VISIT_TYPE_ID,
	--coalesce(ltp.TRACKING_PLATFORM_ID, 0) as TRACKING_PLATFORM_ID,
	nvl(lu.COUNTRY_BUSINESS_ID, 0) as BUSINESS_COUNTRY_ID,
	nvl(lu.COUNTRY_PRIVATE_ID, 0) as PRIVATE_COUNTRY_ID
	--coalesce(geo.COUNTRY_ID, 0) as GEO_COUNTRY_ID
from
	RAW.PUBLIC.STAGE_DATAFEEDS_RAW df
	left join RAW.PUBLIC.LU_USER lu on df.POST_EVAR3 = cast(LEFT(MD5(lu.USER_ID||'sahuVoh5'), 16) as VARCHAR(32))
	left join RAW.PUBLIC.STAGE_DATAFEEDS_MOBILE_ATTRIBUTES mda on df.MOBILE_ID = mda.MOBILE_ID
    /*
    left join #SEED#.LU_ACTIVITY_VISIT_TYPE vt on vt.ACTIVITY_VISIT_TYPE_NAME = case df.POST_PROP16 when 'logged_in' then 'Logged In' when 'soft_logged_in' then 'Soft Logged In' when 'logged_out' then 'Logged Out' end
    left join #SEED#.LU_TRACKING_PLATFORM ltp on ltp.TRACKING_PLATFORM_NAME =
                        case
                            when df.POST_PROP1 = 'wbm' and mda.MOBILE_DEVICE_TYPE = 'Mobile Phone' then 'Web - Small Screen'
                            when df.POST_PROP1 = 'wbm' then 'Web - Big Screen'
                            when df.POST_PROP1 in ('w10m','wbm_w10m') then 'Web - Big Screen'
                            when df.POST_PROP1 in ('iosm','wbm_iosm') then 'iOS'
                            when df.POST_PROP1 in ('andm','wbm_andm') then 'Android'
                        end
	left join #SEED#.LU_COUNTRY geo on lower(df.GEO_COUNTRY) = lower(geo.COUNTRY_CODE_ISO_3)
    */
where
	to_date(df.DATE_TIME) = to_date('20220505','YYYYMMDD') and
    position('20180', df.POST_EVENT_LIST) > 0
limit 100;



--Contact Request Accepted & Declined
select
	to_date(df.DATE_TIME) as ACTIVITY_DATE,
	df.DATE_TIME as ACTIVITY_DATETIME,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT || df.VISIT_PAGE_NUM as ACTIVITY_CODE,
	df.POST_VISID_HIGH || df.POST_VISID_LOW || df.VISIT_NUM || df.VISIT_START_TIME_GMT as VISIT_CODE,
	lu.USER_ID as USER_ID,
	df.POST_PROP1 as APP_NAME,
	coalesce(trim(mda.MOBILE_DEVICE_NAME),trim(df.POST_MOBILEDEVICE),'Unknown') as DEVICE_NAME,
	case
	   when position('20185', df.POST_EVENT_LIST) > 0 then 'Contact Request Accepted'
	   when position('20187', df.POST_EVENT_LIST) > 0 then 'Contact Request Declined'
    end as INTERACTION_TYPE_NAME,
	--coalesce(vt.ACTIVITY_VISIT_TYPE_ID, 0) as ACTIVITY_VISIT_TYPE_ID,
	--coalesce(ltp.TRACKING_PLATFORM_ID, 0) as TRACKING_PLATFORM_ID,
	nvl(lu.COUNTRY_BUSINESS_ID, 0) as BUSINESS_COUNTRY_ID,
	nvl(lu.COUNTRY_PRIVATE_ID, 0) as PRIVATE_COUNTRY_ID
	--coalesce(geo.COUNTRY_ID, 0) as GEO_COUNTRY_ID
from
	RAW.PUBLIC.STAGE_DATAFEEDS_RAW df
	left join RAW.PUBLIC.LU_USER lu on df.POST_EVAR3 = cast(LEFT(MD5(lu.USER_ID||'sahuVoh5'), 16) as VARCHAR(32))
	left join RAW.PUBLIC.STAGE_DATAFEEDS_MOBILE_ATTRIBUTES mda on df.MOBILE_ID = mda.MOBILE_ID
    /*
    left join #SEED#.LU_ACTIVITY_VISIT_TYPE vt on vt.ACTIVITY_VISIT_TYPE_NAME = case df.POST_PROP16 when 'logged_in' then 'Logged In' when 'soft_logged_in' then 'Soft Logged In' when 'logged_out' then 'Logged Out' end
    left join #SEED#.LU_TRACKING_PLATFORM ltp on ltp.TRACKING_PLATFORM_NAME =
                        case
                            when df.POST_PROP1 = 'wbm' and mda.MOBILE_DEVICE_TYPE = 'Mobile Phone' then 'Web - Small Screen'
                            when df.POST_PROP1 = 'wbm' then 'Web - Big Screen'
                            when df.POST_PROP1 in ('w10m','wbm_w10m') then 'Web - Big Screen'
                            when df.POST_PROP1 in ('iosm','wbm_iosm') then 'iOS'
                            when df.POST_PROP1 in ('andm','wbm_andm') then 'Android'
                        end
	left join #SEED#.LU_COUNTRY geo on lower(df.GEO_COUNTRY) = lower(geo.COUNTRY_CODE_ISO_3)
    */
where
	to_date(df.DATE_TIME) = to_date('20220505','YYYYMMDD') and
	(
        position('20185', df.POST_EVENT_LIST) > 0
        or
        position('20187', df.POST_EVENT_LIST) > 0
	)
limit 100;