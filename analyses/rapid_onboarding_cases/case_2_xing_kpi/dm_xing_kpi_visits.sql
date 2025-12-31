/*
    XING VISITS
    target table: DM_XING_KPI
*/
select
    trunc(d.ACTIVITY_DATE,'IW') as PERIOD_START,
    case
        when c.REGION_NAME in ('DACH', 'Non-DACH') then c.REGION_NAME
        when gc.REGION_NAME in ('DACH', 'Non-DACH') then gc.REGION_NAME
        else 'Non-DACH'
    end as REGION_NAME,
    p.TRACKING_PLATFORM_NAME,
    'total' as ACTIVITY_VISIT_TYPE_NAME,
    'XIN-04' as KPI_ID,
    'Xing Visits' as KPI_NAME,
    'weekly' as AGGREGATE_LEVEL,
    count(distinct d.VISIT_CODE) as KPI_VALUE,
    current_timestamp as ETL_ROW_CREATE_DATE
from
    DWH_ANALYTICS.F_USER_ACTIVITY d
    left join DWH_ANALYTICS.LU_TRACKING_PLATFORM p on d.TRACKING_PLATFORM_ID = p.TRACKING_PLATFORM_ID
    left join DWH_ANALYTICS.LU_COUNTRY c on d.BUSINESS_COUNTRY_ID = c.COUNTRY_ID
    left join DWH_ANALYTICS.LU_COUNTRY gc on d.GEO_COUNTRY_ID = gc.COUNTRY_ID
where true
    and d.TRACKING_PLATFORM_ID <> 7 --remove email tracking
group by
    1, cube(2,3)
;

update DWH_ANALYTICS.DM_XING_KPI set REGION_NAME = 'total' where REGION_NAME is null;
update DWH_ANALYTICS.DM_XING_KPI set TRACKING_PLATFORM_NAME = 'total' where TRACKING_PLATFORM_NAME is null;