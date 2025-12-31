<!-- 	Individual Columns -->

<!-- Salaries -->
{% docs mart_jobs__column_salary_range_start_docs %} The range start value of a salary entry. Logic of choosing the salary columns and examples can be found in Jira BICC-8215. {% enddocs %}
{% docs mart_jobs__column_salary_range_end_docs %} The range end value of a salary entry. Logic of choosing the salary columns and examples can be found in Jira BICC-8215. {% enddocs %}
{% docs mart_jobs__column_salary_range_median_docs %} The range median value of a salary entry. Logic of choosing the salary columns and examples can be found in Jira BICC-8215. {% enddocs %}
{% docs mart_jobs__column_salary_fixed_docs %} The fixed salary for a job posting. {% enddocs %}
{% docs mart_jobs__column_salary_type_docs %} The type of salary for a job posting. Estimated or User. {% enddocs %}
{% docs mart_jobs__column_salary_currency_docs %} The currency of the proposed/ estimated salary for a job posting. {% enddocs %}

{% docs mart_jobs__column_posting_id_docs %} Unique identifier for the Job Posting. {% enddocs %}

<!-- Industries -->
{% docs mart_jobs__column_industry_id_docs %} Unique identifier of an industry. {% enddocs %}
{% docs mart_jobs__column_industry_name_docs %} The industry name in English. {% enddocs %}
{% docs mart_jobs__column_industry_name_de_docs %} The industry name in German. {% enddocs %}
{% docs mart_jobs__column_industry_group_id_docs %} The industry group ID. With it, several industries can be grouped together. {% enddocs %}
{% docs mart_jobs__column_industry_group_docs %} The industry group (in English), according to the industry group ID. {% enddocs %}
{% docs mart_jobs__column_industry_group_de_docs %} The industry group (in German), according to the industry group ID. {% enddocs %}
{% docs mart_jobs__column_industry_account_wording_docs %} The industry account wording (in German). {% enddocs %}

<!-- Posting Orders -->
{% docs mart_jobs__column_order_id_docs %} The order ID associated with a Job Posting. {% enddocs %}
{% docs mart_jobs__column_order_contract_list_docs %} List of contracts associated with the order. {% enddocs %}
{% docs mart_jobs__column_order_amount_docs %} Amount associated with the order. {% enddocs %}
{% docs mart_jobs__column_order_service_offering_group_docs %} Job Posting's group id associated to their Onlyfy product. See [Confluence documentation](https://new-work.atlassian.net/wiki/spaces/JOBSPLAT/pages/39359934/Jobs+Service+Offering+Groups). {% enddocs %}
{% docs mart_jobs__column_order_duration_docs %} Duration of the order in days. {% enddocs %}
{% docs mart_jobs__column_order_created_at_utc_docs %} Timestamp in UTC when the order was created. {% enddocs %}
{% docs mart_jobs__column_order_contingent_used_docs %} The number of job postings that were activated in the order. {% enddocs %}
{% docs mart_jobs__column_order_product_type_docs %} Mapped product type based on the order type. {% enddocs %}
{% docs mart_jobs__column_order_is_billing_strategy_online_docs %} Billing strategy for the order. {% enddocs %}
{% docs mart_jobs__column_order_is_pay_per_click_docs %} Boolean flag indicating if the order is pay-per-click. {% enddocs %}
{% docs mart_jobs__column_order_is_paid_docs %} Boolean flag indicating if the order has been paid. {% enddocs %}
{% docs mart_jobs__column_order_is_online_sales_type_docs %} Flag that is calculated using the logic provided by Xing: if orders.is_billing_strategy_online = true or organizations.is_b2b_online_client = 1 then TRUE. {% enddocs %}
{% docs mart_jobs__column_order_business_model_docs %} Calculated using the logic provided by Onlyfy. Possible values: Unpaid, Clickprice, Fixprice, Unknown. Calculated based on orders.billing_strategy, orders.is_pay_per_click and organizations.is_b2b_online_client. {% enddocs %}
{% docs mart_jobs__column_order_contract_start_date_docs %} Start date of the contract. {% enddocs %}
{% docs mart_jobs__column_order_contract_end_date_docs %} End date of the contract. {% enddocs %}
{% docs mart_jobs__column_order_is_third_party_jobs_partner_docs %} Boolean flag indicating if the order is from a third-party jobs partner. {% enddocs %}

<!-- Organizations -->
{% docs mart_jobs__column_organization_id_docs %} Unique identifier of an organization. {% enddocs %}
{% docs mart_jobs__column_organization_name_docs %} Organization name. {% enddocs %}
{% docs mart_jobs__column_organization_salesforce_id_docs %} Salesforce ID associated with the organization. {% enddocs %}
{% docs mart_jobs__column_organization_client_id_docs %} B2B client ID associated with the organization. {% enddocs %}
{% docs mart_jobs__column_organization_is_online_client_docs %} Boolean flag indicating if the organization is an online B2B client. {% enddocs %}

<!-- Postings -->
{% docs mart_jobs__column_paid_type_docs %} Job Posting's paid type classification of either 'paid_high_revenue', 'paid_low_revenue' or 'unpaid'. {% enddocs %}
{% docs mart_jobs__column_posting_editor_user_id_docs %} XING user id of the posting editor. {% enddocs %}
{% docs mart_jobs__column_posting_editor_name_docs %} The editor first name and last name concatenated. {% enddocs %}
{% docs mart_jobs__column_posting_editor_business_email_docs %} The business email of the editor. {% enddocs %}
{% docs mart_jobs__column_posting_is_onlyfy_one_job_ad_posting_docs %} Flag that indicates if the posting is present in prescreen. {% enddocs %}
{% docs mart_jobs__column_posting_sales_channel_docs %} Determines if the contract was won via online channels only (with no Sales involvement at all), via offline channels only or a mix of both ("Multichannel"). {% enddocs %}
{% docs mart_jobs__column_posting_allocated_contract_number_docs %} Allocated unique contract number, also for postings having more than one assigned contract. {% enddocs %}
{% docs mart_jobs__column_contract_product_name_docs %} Contract product name based on allocated contract number. {% enddocs %}
{% docs mart_jobs__column_contract_has_money_back_guarantee_docs %} Money back guarantee flag based on allocated contract number. {% enddocs %}
{% docs mart_jobs__column_posting_is_third_party_jobs_partner_docs %} Reactivated flag (per 2025-02) for all CPC postings. {% enddocs %}
{% docs mart_int_jobs__column_activation_week_docs %} Posting's week of activation. {% enddocs %}
{% docs mart_int_jobs__column_activation_month_docs %} Posting's month of activation. {% enddocs %}
{% docs mart_jobs__column_visibility_flag_docs %} Whether the posting has been visible yet. Value doesn't change when the posting is deactivated. {% enddocs %}


{% docs mart_jobs__column_posting_company_id_docs %} The ID of the company that posted. {% enddocs %}
{% docs mart_jobs__column_posting_company_name_docs %} The name of the company that posted. {% enddocs %}
{% docs mart_jobs__column_posting_url_docs %} The job posting URL. {% enddocs %}
{% docs mart_jobs__column_posting_job_code_from_editor_docs %} Job code for identification of the job, set from the editor. {% enddocs %}
{% docs mart_jobs__column_posting_product_type_docs %} The name of the job posting product type, hard-coded. For ex. Pro, Ultimate, etc. One of the Top-Paid (Onlyfy) products. {% enddocs %}
{% docs mart_jobs__column_posting_title_docs %} The job title of a job posting, for ex. Senior BI Manager. {% enddocs %}
{% docs mart_jobs__column_posting_country_docs %} ISO country code, made up of two letter (for example, DE for Germany). {% enddocs %}
{% docs mart_jobs__column_posting_city_docs %} The name of the city for the job posting. {% enddocs %}
{% docs mart_jobs__column_posting_region_docs %} The name of the region for the job posting. {% enddocs %}

{% docs mart_jobs__column_posting_activated_at_docs %} The date when the job posting was activated. {% enddocs %}
{% docs mart_jobs__column_posting_expired_at_docs %} Timestamp of the expiration of the job posting. {% enddocs %}
{% docs mart_jobs__column_posting_total_visible_days_docs %} The total number of days the posting was visible. {% enddocs %}
{% docs mart_jobs__column_apply_type_docs %} One of the Apply button types (e.g., 'url', 'instant_apply', etc.) {% enddocs %}

<!-- Career level -->
{% docs user__career_level_id_docs %} User career level ID. {% enddocs %}
{% docs user__career_level_de_docs %} User career level (German). {% enddocs %}
{% docs user__career_level_en_docs %} User career level (English). {% enddocs %}

<!-- Roles -->
{% docs mart_jobs__role_id_docs %} Unique identifier of the entity (role). {% enddocs %}
{% docs mart_jobs__role_name_de_docs %} Job role name (German). {% enddocs %}
{% docs mart_jobs__role_name_en_docs %} Job role name (English). {% enddocs %}
{% docs mart_jobs__role_reference_position_docs %} Minimum reference position for the job role. {% enddocs %}

<!-- Apply Completions -->
{% docs mart_jobs__column_apply_completion_sk_docs %} Unique identifier for the Apply Completion event. {% enddocs %}
{% docs mart_jobs__column_apply_completion_unique_id_docs %} In theory, an Apply Completion (AC) can only happen once per user and posting. In logged-out or unknown users, the visit and posting IDs are used instead. When calculating the distinct apply completions, this column is more reliable than the SK one.{% enddocs %}

<!-- Apply Intentions -->
{% docs mart_jobs__column_apply_intention_sk_docs %} Unique identifier for the Apply Intention event. {% enddocs %}

<!-- Bookmarks -->
{% docs mart_jobs__column_bookmark_sk_docs %} Unique identifier for the Bookmark event. {% enddocs %}
{% docs mart_jobs__column_bookmark_updated_at_utc_docs %} Datetime the bookmark state was updated. {% enddocs %}
{% docs mart_jobs__column_bookmark_activated_at_utc_docs %} Datetime the bookmark was activated. {% enddocs %}
{% docs mart_jobs__column_bookmark_is_active_docs %} Whether the job posting bookmark is active or not. {% enddocs %}
{% docs mart_jobs__column_bookmark_state_docs %} Bookmark's state. {% enddocs %}

<!-- Cardviews -->
{% docs mart_jobs__column_card_view_sk_docs %} Unique identifier for the Card View event. {% enddocs %}

<!-- Job Detail Views -->
{% docs mart_jobs__column_detail_view_sk_docs %} Unique identifier for the Detail View event. {% enddocs %}
{% docs mart_jobs__column_job_posting_view_internal_traffic_channel_id_docs %} Internal traffic channel ID based on JB code extraction from origin evar hit ID. {% enddocs %}


<!-- Common Events / Tracking-->
{% docs mart_jobs__column_event_created_at_utc_docs %} Event's creation timestamp in UTC time format. {% enddocs %}
{% docs mart_jobs__column_event_created_at_cet_docs %} Event's creation timestamp in CET time format. {% enddocs %}
{% docs mart_jobs__column_event_visit_id_docs %} Unique identifier for the (Adobe) web visit. {% enddocs %}
{% docs mart_jobs__column_event_activity_id_docs %} Business key for activity. {% enddocs %}
{% docs mart_jobs__column_event_visitor_id_docs %} Business key for the visit. {% enddocs %}
{% docs mart_jobs__column_event_cust_visitor_id_docs %} Business key for the visit (cust). {% enddocs %}
{% docs mart_jobs__column_event_login_status_docs %} Indicates the login state in which the user performed the event, i.e, logged in, logged out or soft logged in. {% enddocs %}
{% docs mart_jobs__column_event_activity_platform_docs %} Platform of the activity where the event occured. {% enddocs %}
{% docs mart_jobs__column_activity_platform_aggregated_docs %} Platform of the activity, but 'web - big screen' and 'web - small screen' are grouped into 'web'. {% enddocs %}
{% docs mart_jobs__column_event_traffic_source_id_docs %} ID of the source. {% enddocs %}
{% docs mart_jobs__column_event_page_name_docs %} The page, screen or view the event occurred on. {% enddocs %}
{% docs mart_jobs__column_event_notification_type_docs %} Additional context of the page, screen or event. {% enddocs %}
{% docs mart_jobs__column_event_element_name_docs %} The element that the event occurred on/with. {% enddocs %}
{% docs mart_jobs__column_event_element_detail_docs %} The element's additional detail information, if further specified. {% enddocs %}
{% docs mart_jobs__column_event_is_native_docs %} Flag indicating whether it is native traffic. {% enddocs %}
{% docs mart_jobs__column_event_is_web_docs %} Flag indicating whether it is web traffic. {% enddocs %}
{% docs mart_jobs__column_event_is_backend_docs %} Flag indicating whether it is backend traffic. {% enddocs %}

{% docs mart_jobs__column_nwt_sent_by_docs %} The section that generated the tracking event (use lowercase, e.g. networkc5l7tn2k50, bmaraiq5fo, etc.). {% enddocs %}
{% docs mart_jobs__column_nwt_device_id_docs %} The identifier that serves as a fallback when the user is logged out. {% enddocs %}
{% docs mart_jobs__column_nwt_event_name_docs %} The specific name of the event (e.g. clicked, accepted, applied, responded, viewed_screenv3s46zvfqf, downloaded, created, visited, etc.). {% enddocs %}
{% docs mart_jobs__column_nwt_referrer_docs %} The URL that linked to the current page (set by the SDK). {% enddocs %}
{% docs mart_jobs__column_nwt_query_docs %} A query-formatted and URL-encoded string for attributes. {% enddocs %}
{% docs mart_jobs__column_nwt_flags_docs %} A list of Boolean On/Off properties that indicate aspects about the event or user. {% enddocs %}
{% docs mart_jobs__column_nwt_user_agent_docs %} The UserAgent information of the browser. {% enddocs %}
{% docs mart_jobs__column_nwt_tracking_token_docs %} Identifier that facilitates joining Delivery and Interaction events from multiple applications in post processing. Follows the format <source_label>:<request_identifier>:<custom_suffix>. {% enddocs %}
{% docs mart_jobs__column_nwt_screen_url_docs %} URL of the displayed screen (should not contain queries/fragments). {% enddocs %}
{% docs mart_jobs__column_nwt_context_id_docs %} The context from which a user navigated into the messenger (e.g. birthday, contact_requested, company_anniversary, media_preview, search_profile, etc.). {% enddocs %}
{% docs mart_jobs__column_nwt_event_sk_docs %} Surrogate key obtained by combining the dt field (integer with the ETL execution date in the format YYYYMMDD) with a uuid function and hashing it. Replaced by a new surrogate key structure generated by hashing all non-technical fields plus row_number to handle duplicates from the source. {% enddocs %}
{% docs mart_jobs__column_nwt_experiment_docs %} Contains experiment_name|displayed_assignment|received_assignment. {% enddocs %}
{% docs mart_jobs__column_nwt_job_posting_id_from_urn_docs %} The ID of the job posting extracted from the URN. {% enddocs %}
{% docs mart_jobs__column_nwt_job_posting_id_from_url_docs %} The ID of the job posting extracted from the URL. {% enddocs %}

<!-- Performance -->
{% docs mart_jobs__column_performance_metric_job_detail_views_docs %} Job Detail Views. {% enddocs %}
{% docs mart_jobs__column_performance_metric_bookmarks_docs %} Bookmarks. {% enddocs %}
{% docs mart_jobs__column_performance_metric_apply_intentions_docs %} Apply Intentions. {% enddocs %}
{% docs mart_jobs__column_performance_metric_unique_visitors_docs %} Unique Visitors. If the data is daily, a unique visitor is counted only the first day the user performed it. {% enddocs %}
{% docs mart_jobs__column_performance_metric_daily_unique_visitors_docs %} Unique Visitors, given a day. {% enddocs %}
{% docs mart_jobs__column_performance_metric_apply_intentions_url_apply_docs %} Apply Intentions via the company's URL button. {% enddocs %}
{% docs mart_jobs__column_performance_metric_apply_intentions_instant_apply_docs %} Apply Intentions via the instant apply button. {% enddocs %}
{% docs mart_jobs__column_performance_metric_applications_docs %} Distinct Apply Completions. If the data is daily, an apply completion is counted only the first day the user performed it. {% enddocs %}
{% docs mart_jobs__column_performance_metric_daily_applications_docs %} Distinct Apply Completions, given a day. {% enddocs %}
{% docs mart_jobs__column_performance_metric_impressions_docs %} Job Card Views. {% enddocs %}


<!-- Traffic -->
{% docs mart_jobs__column_traffic_channel_docs %} One of the external traffic channels (e.g., 'Direct', 'Paid', 'Mail', etc.) {% enddocs %}
{% docs mart_jobs__column_traffic_channel_group_docs %} One of the external traffic channel groups. Here, external channels are grouped into a more general classification. At the same time, the paid campaings are considered to form groups. See [xing_stg_jobs__external_traffic_channel](https://github.com/new-work/dbt/blob/main/models/xing/jobs/jobs-data-model/_staging/xing_stg_jobs__external_traffic_channel.sql). {% enddocs %}
{% docs mart_jobs__column_traffic_channel_class_docs %} External traffic channels are groups into their most general classification: 'Organic', 'Sponsored' and 'Unknown'. {% enddocs %}

<!-- Others -->
{% docs mart_jobs__column_user_id_docs %} Unique identifier for user. {% enddocs %}
{% docs mart_jobs__column_country_code_docs %} Visit's geo location. One possible country code. {% enddocs %}
{% docs mart_jobs__column_dach_flag_docs %} Whether the country belongs to the DACH region (Germany, Austria, Switzerland) or not. {% enddocs %}
{% docs mart_jobs__column_device_type_docs %} Type of the device for the activity. {% enddocs %}
{% docs mart_jobs__column_dbt_updated_at_utc_docs %} Technical field to indicate the timestamp when the dbt model was updated. {% enddocs %}

<!-- 	Full Models -->

{% docs int_jobs__high_revenue_postings_docs %}

## This model

This is the main table for identifying the Top-Paid Postings on XING. It is also the truth source
for Performance Marketing.


## Owners & Stakeholders

- The model is (at the time of writing) maintained by the Passive Sourcing (Jobs) Central Analytics team.
- Stakeholders may come from XING Performance Marketing, Active Sourcing (ex-Onlyfy) Central Analytics and BI.


## Special columns

- `high_revenue_type_id`: Different types of Top-Paid Postings. _E.g._ either the old or new product types,
  or postings belonging to certain companies for whom we would like to see high performance, etc.

| ID  | Label                  | Description                                                                                                                                                                                                           |
| --- | ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Initial def. of TPPs   | The initial, core definition of top-paid postings (see [here](https://source.xing.com/jobs-analytics/business-kpis/blob/ba96de020c16ae6867bdbbd56f051fdff3715b5d/project/UpdateHighRevenuePostings.scala#L130-L174).) |
| 2   | NEW WORK SE            | Postings that are from NEW WORK SE and its daughter companies.                                                                                                                                                        |
| 3   | Agency Boost / ZP 2022 | Postings from selected companies (agencies) to whom we would like to showcase before the ZP fare the impact of boosting their postings. The selection of postings is based on a set of order IDs.                     |
| 4   | XTS Postings           | Postings from XING Talent Services.                                                                                                                                                                                   |
| 5   | Jobs Service Offering  | New definition of top-paid job postings that fall into one of the premium/pro/... service offering groups.                                                                                                            |
| 6   | Low-paid Boosting      | For selected customers, we also boost low-paid postings for a certain amount of time (e.g. Workwise case). Those postings are however not added to the table.                                                         |

For the calculation of ID 1 postings some contract tables are being joined to get the contract id. This is 
a legacy approach and has been removed from the current code and transformed into a seed (stg_jobs__high_revenue_postings_new_with_legacy_strategy).
This is also because ID 1 postings have not been created any more since 2024-02, so the logic will not be adjusted. https://new-work.atlassian.net/browse/XA-6526

- `service_offering_group_id`: The ID of the service offering group which decides about the type of
  boosting and other premium (_e.g._ Top4Top settings) that we offer for the job posting. [Specific information
  can be found here](https://new-work.atlassian.net/wiki/x/vpVYAg).

| Service offering group | Onlyfy products        |
| ---------------------- | ---------------------- |
| 0                      | Old portfolio, Core360 |
| 1                      | Core                   |
| 2                      | Pro                    |
| 3                      | Ultimate               |
| 4                      | Essential              |
| 5                      | Core15                 |
| 6                      | TalentBoostPro         | 
| 7                      | TalentBoostUltimate    |


## Dependencies

The model depends mainly on the central postings table and (Onlyfy) salesforce tables.


## dbt Jobs and Automations

- **dbt Job**: [Link](https://emea.dbt.com/deploy/74/projects/309/jobs/90428)
- **Airflow DAG**: [xing_jobs__int_job_models](https://airflow-bi.xing.io/dags/xing_jobs__int_job_models/grid)


{% enddocs %}


<!-- ----------------------------------------------------------------------------------------------------------------------------------------------------------------- -->

{% docs int_jobs__postings_contract_mapping_docs %}

## This model

Establishes a unique connection between a job posting and a sales contract. Currently the contracts for a posting 
are being added manually by colleagues to the offer via xing admin (dimension order_contract_list in posting table). 
This join differs 3 cases
- case #1: Orders having only 1 contract --> Single contract will be directly assigned to all postings having this order id.
- case #2: Orders having multiple contracts with unreliable quantity. As at least one of the contracts does not have a reliable
           quantity (e.g. 360 contracts where customers can create n postings) we need to join via date:
           posting creation date between start and end date
           There are some exceptions in the data. For details please check the case in orders_with_multiple_contracts_date_assignment_step2
- case #3: Orders having multiple contracts with reliable quantity. Assignment will be triggered via cumulated posting and contract order.
           Main logic can be found in orders_with_multiple_contracts_cumulated_approach.
- case #4: Edge case of case #3: For some orders the number of activated postings is higher than the cumulated quantity of all contracts for this order. 
           In this case all postings for this order having a bigger cumulated amount than the max of all contracts are being assigned to the moct recent contract within the order.           


## Owners & Stakeholders

- The model is maintained by the Passive Sourcing (Jobs) Analytics team.
- Stakeholders may come from all units which need to join sales data with postings.


## Special columns

- `contract_number`: Number of the contract the posting has been assigned to

- `contract_product_name`: Product name of the contract

- `has_money_back_guarantee`: Flag if postings of the order are having the money back guarantee which was introduced in march 2025


## Dependencies

The model depends mainly on the sales datamart and job postings.


## dbt Jobs and Automations

Has been added to posting job
- **dbt Job**: [Link](https://emea.dbt.com/deploy/74/projects/309/jobs/90429)
- **Airflow DAG**: [xing_jobs__int_job_models](https://airflow-bi.xing.io/dags/xing_jobs__int_job_models/grid)

{% enddocs %}