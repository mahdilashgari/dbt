/*
    XING USER ACTIVITY - DETAIL (INTERMEDIATE MODEL)
*/
select
    a.ACTIVITY_DATE as DATE_ID,
    a.USER_ID,
    case usr.REGION_BUSINESS when 'DACH' then 'DACH' else 'Non-DACH' end as REGION_NAME,
    p.TRACKING_PLATFORM_NAME,
    case max(vt.ACTIVITY_VISIT_TYPE_NAME = 'Logged In') when 1 then 'Logged In' else 'Logged Out' end as ACTIVITY_VISIT_TYPE_NAME, --take logged-in as priority per day
    current_timestamp as ETL_ROW_CREATE_DATE
from
    DWH_ANALYTICS.F_USER_ACTIVITY a
    left join DWH_ANALYTICS.LU_USER usr on a.USER_ID = usr.USER_ID
    left join DWH_ANALYTICS.LU_TRACKING_PLATFORM p ON a.TRACKING_PLATFORM_ID = p.TRACKING_PLATFORM_ID
    left join DWH_ANALYTICS.LU_ACTIVITY_VISIT_TYPE vt on a.ACTIVITY_VISIT_TYPE_ID = vt.ACTIVITY_VISIT_TYPE_ID
where true
    and a.TRACKING_PLATFORM_ID <> 7 --remove email tracking
group by
    1,2,3,4
;