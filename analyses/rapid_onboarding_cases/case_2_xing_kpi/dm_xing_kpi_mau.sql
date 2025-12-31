/*
    MONTHLY ACTIVE USER
    target table: DM_XING_KPI
*/
select
    trunc(d.DATE_ID, 'MM') as PERIOD_START,
    d.REGION_NAME as REGION_NAME,
    d.TRACKING_PLATFORM_NAME,
    d.ACTIVITY_VISIT_TYPE_NAME,
    'XIN-08' as KPI_ID,
    'MAU' as KPI_NAME,
    'monthly' as AGGREGATE_LEVEL,
    count(distinct USER_ID) as KPI_VALUE,
    current_timestamp as ETL_ROW_CREATE_DATE
from
    DWH_ANALYTICS.DM_XING_USER_ACTIVITY_DETAIL d
where true
    and d.REGION_NAME = 'DACH'
group by
    1, cube(2,3,4)
;

update DWH_ANALYTICS.DM_XING_KPI set REGION_NAME = 'total' where REGION_NAME is null;
update DWH_ANALYTICS.DM_XING_KPI set MEMBERSHIP_CATEGORY_NAME = 'total' where MEMBERSHIP_CATEGORY_NAME is null;
update DWH_ANALYTICS.DM_XING_KPI set TRACKING_PLATFORM_NAME = 'total' where TRACKING_PLATFORM_NAME is null;
update DWH_ANALYTICS.DM_XING_KPI set ACTIVITY_VISIT_TYPE_NAME = 'total' where ACTIVITY_VISIT_TYPE_NAME is null;