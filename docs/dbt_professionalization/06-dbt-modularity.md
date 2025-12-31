# 1. Modular Data Modelling

- [1. Modular Data Modelling](#1-modular-data-modelling)
  - [1.1. Introduction](#11-introduction)
    - [1.1.1. Benefits of Modularity in dbt](#111-benefits-of-modularity-in-dbt)
    - [1.1.2. Structuring Models for Modular Development](#112-structuring-models-for-modular-development)
    - [1.1.3. Modularity in dbt: Jinja Functions and Macros](#113-modularity-in-dbt-jinja-functions-and-macros)
  - [1.2. Using Jinja functions](#12-using-jinja-functions)
    - [1.2.1. What is Jinja](#121-what-is-jinja)
    - [1.2.2. Expressions `{{ ref() }}`, `{{ config() }}`, `{{ this }}`](#122-expressions--ref---config---this-)
    - [1.2.3. Statements `{% set %}`, `{% if.. %}`, `{% for.. %}`](#123-statements--set---if---for-)
  - [1.3. Using Macros](#13-using-macros)
    - [1.3.1. BI Macros](#131-bi-macros)
    - [1.3.2. DBT Native Packages](#132-dbt-native-packages)
      - [1.3.2.1. Generic Tests Macros](#1321-generic-tests-macros)
      - [1.3.2.2. SQL Generators](#1322-sql-generators)
  - [1.4. Let's put all together](#14-lets-put-all-together)


## 1.1. Introduction

Modularity is a fundamental principle in software development and data engineering, ensuring that complex systems are built as a collection of smaller, reusable components. In the context of **dbt**, modularity plays a crucial role in maintaining clean, efficient, and scalable analytics workflows.

As data models evolve, the need for well-structured, maintainable code becomes more evident. A modular approach in dbt allows analysts and engineers to break down monolithic SQL scripts into manageable components, making it easier to debug, test, and extend. This principle is particularly relevant when working with dynamic SQL transformations, where reusability and clarity are essential.

### 1.1.1. Benefits of Modularity in dbt

1. **Code Reusability**: 
   - By breaking logic into reusable components such as macros and Jinja functions, teams can avoid redundant SQL queries and improve maintainability.
   
2. **Improved Maintainability**:
   - Modular code makes debugging and testing easier by isolating logic into smaller, self-contained units.
   
3. **Scalability**:
   - As datasets and business requirements grow, modular code facilitates incremental changes without significant rework.
   
4. **Consistency Across Models**:
   - Standardizing transformations and logic across models ensures that KPIs and business rules are consistently applied.
   
5. **Enhanced Collaboration**:
   - Teams can work on different parts of the data pipeline independently while ensuring seamless integration.


### 1.1.2. Structuring Models for Modular Development

A key aspect of modularity in dbt is organizing data models into structured layers, typically including:

- **Staging Models (`stg_`)**: These models clean and standardize raw source data, ensuring consistency and usability.
- **Intermediate Models (`int_`)**: These act as transformation layers, combining and reshaping staging data before final aggregation.
- **Data Mart Models (`dm_`)**: These contain business-ready tables optimized for reporting and analytics, ensuring efficient data access for end-users.

By following this layered approach, teams can separate concerns, improve model readability, and enhance reusability across different transformations. (See *[dbt docs: How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)*).


### 1.1.3. Modularity in dbt: Jinja Functions and Macros

One of dbt’s most powerful features is its support for Jinja, a templating language that enables dynamic SQL generation. Through the use of Jinja functions and macros, dbt allows users to create flexible, reusable logic that can be applied across multiple models. This eliminates hardcoded SQL logic and fosters a modular approach to data transformation.

In the following sections, we will explore how Jinja functions and macros can be used to enhance modularity in dbt, providing real-world examples to illustrate their effectiveness in practice.


## 1.2. Using Jinja functions

### 1.2.1. What is Jinja

Jinja is a template engine which in DBT can be used alongside SQL to make your code more dynamic. With Jinja you can create text, define variables, establish conditions which can be then passed or applied onto your sql file (spoiler alert: if you have used something like `{{ ref() }}` or `{{ config() }}` in dbt then congratulations, you have already used Jinja). 

Jinja templates can be recognised by their delimers `{}` and can be broken down into 3 main categories:
- **Expressions {{ ... }}**: used when you want to output a string. You can use expressions to reference variables and call macros.
- **Statements {% ... %}**: Statements don't output a string but control flows, for example, to set up for loops and if statements, to set or modify variables, or to define macros.
- **Comments {# ... #}**: Jinja comments are used to prevent the text within the comment from executing or outputing a string. Don't use `--` for comments within the Jinja brackets!

In the coming sections we list the most common and useful Jinja expressions and statements that we will find while coding, but this is far from being a full list of possibilities. Therefore we refer you to some great resources where we encourage you to read through the full list of both native and dbt built-in Jinja functions:

- datacoves: [Ultimate dbt Jinja Cheat Sheet](https://datacoves.com/post/dbt-jinja-cheat-sheet?utm_source=chatgpt.com)
- datacoves: [Ultimate dbt Jinja Functions Cheat Sheet](https://datacoves.com/post/dbt-jinja-functions-cheat-sheet?utm_source=chatgpt.com)
- dbt docs: [Jinja and macros](https://docs.getdbt.com/docs/build/jinja-macros?utm_source=chatgpt.com)
- dbt docs: [dbt Jinja functions](https://docs.getdbt.com/reference/dbt-jinja-functions)


### 1.2.2. Expressions `{{ ref() }}`, `{{ config() }}`, `{{ this }}`

The following is a list of dbt built-in Jinja functions: 

- `{{ ref() }}` references a model and builds its dependency graph. 

    ```sql 
    select * from {{ ref(xing_model_name)}}
    ```

- `{{ config() }}` sets up configurations for a given model (e.g. warehouse size, model materialisation, incremental strategy, unique keys).
  
    ```sql 
    {{ config( materialized = 'table', snowflake_warehouse = 'xing_dbt_wh_large' ) }}
    ```

- `{{ this }}` is a representation of the current model. `this` can be thought of as equivalent to `ref('<the_current_model>')`, and is a neat way to avoid circular dependencies. For instance, `this` can be used for incremental strategy in a `where` statement.
   
    ```sql 
    {# Our model called example_data_mart builds on top 
    of xing_model_name, based on the condition that the 
    xing_model_name event_time has to be greater than the 
    max event_time of the example_data_mart model #}

    {{ config( 
        materialized = 'table',
        snowflake_warehouse = 'xing_dbt_wh_large', 
        materialized = 'incremental'
    )}}

    select * 
    from {{ ref(xing_model_name)}}

    {% if is_incremental() %}
    where event_time > (select max(event_time) from {{ this }})
    {% endif %}
    ```


### 1.2.3. Statements `{% set %}`, `{% if.. %}`, `{% for.. %}` 

A **set expression** refers to the use of the set Jinja function, which allows you to create a collection of values that can be used later in your templates or SQL queries. The set expression is especially useful for defining lists or arrays that you want to iterate over or reference multiple times within your dbt models, macros, or analyses. For example, defining a given date from which you want to filter the data, will prevent unexpected results due to typos. 

```sql
{% set date_filter = '2023-01-01' %}


with fct_conversations as ( 

select * 
from {{ ref('onlyfy_fct_product_usage__xtmp_conversations_by_candidate') }} 
where reporting_date >= '{{ date_filter }}'

)

select * from fct_conversations
```

A **for loop** is a control flow statement that allows you to iterate over a sequence of iteratable objects. For instance: 

```python 
# This is a list of objects
conversation_source_list = ['direct_message', 'multimessage', 'campaign']

# A for loop will in this case access each element of the list.  
for conversation_source in conversation_source_list: 
    print(conversation_source)

# Output:
# direct_message
# multimessage
# campaign
```

... and now we might want to make each element in upper case: 

```python 
for conversation_source in conversation_source_list: 
    print(conversation_source.upper())

# Output:
# DIRECT_MESSAGE
# MULTIMESSAGE
# CAMPAIGN
```

Using `if` and `for` loops in your sql scripts not only makes them more legible but also less error-prone. 

Let's consider the following use case: You need to create a reporting table with a daily and monthly aggregation of given metrics. A union all of 2 CTEs will be necessary to generate the time dimensional aggregation column. 

```sql
with fct_conversations as ( 

select *
from analytics.test_mario_iuliano.fct_product_usage__xtmp_conversations_by_candidate 
where reporting_date >= '2023-01-01'

), 

fct_conversations_union as (

    select 
        'daily' as time_agg,
        reporting_date  as period_date,
        recruiter_xtmp_company_id, 
        sum(xtmp_conversations_started) as conversations_started,
        sum(xtmp_conversations_replied) as conversations_replied
    from fct_conversations
    group by all
    
     union all 

    select 
        'monthly' as time_agg,
        date_trunc('month', reporting_date)  as period_date,
        recruiter_xtmp_company_id, 
        sum(xtmp_conversations_started) as conversations_started,
        sum(xtmp_conversations_replied) as conversations_replied
    from fct_conversations
    group by all
    
)

select * from fct_conversations_union
```

Via Jinja we can: 

```sql
{% set time_agg = ['daily','monthly'] %} -- define required time aggregation iteratable object

with fct_conversations as ( 

select *
from {{ ref('onlyfy_fct_product_usage__xtmp_conversations_by_candidate') }} 

), 

fct_conversations_union as (
    -- Iterate through the time aggregation elements and apply a single transformation CTE supported by if condition
    {% for ta in time_agg %} -- for each item of the time_agg list (daily and monthly)

    select 
        '{{ ta }}' as time_agg, -- this will just return the string 'daily' at the first iteration and then 'monthly'
        -- The following statement will return the reporting date when the element of the list is 'daily' 
        -- When the element switches to 'monthly', the reporting date will be truncated to month
        {% if ta == 'daily' %} reporting_date {% else %} date_trunc('month', reporting_date) {% endif %} as period_date,
        recruiter_xtmp_company_id, 
        sum(xtmp_conversations_started) as conversations_started,
        sum(xtmp_conversations_replied) as conversations_replied
    from fct_conversations
    group by all
    
    {% if not loop.last %} union all {% endif %}

    {% endfor %}
)

select * from fct_conversations_union
```

> Note on variables: **We recommend setting variables at the top of a model, as it helps with readability, and enables you to reference the list in multiple places if required** ([DBT - Use Jinja to improve your SQL code](https://docs.getdbt.com/guides/using-jinja?step=4))

Another example where Jinja can significantly reduce redundancy and lines of code, is represented by reporting tables with a KPI name column (ideal for Tableau data sources). 

In this example, we will create a set a KPI Name and KPI value column.

```sql 
{% set date_filter = '2023-01-01' %} 
{% set time_agg = ['daily','monthly'] %}
{% set kpi_names = ['xtmp_conversations_started',
                    'xtmp_conversations_started_30d', 
                    'xtmp_conversations_replied', 
                    'xtmp_conversations_replied_30d', 
                    'xtmp_conversations_read'] %}


with fct_conversations as ( 

select *
from {{ ref('onlyfy_fct_product_usage__xtmp_conversations_by_candidate') }} 
where reporting_date >= '{{ date_filter }}'

), 

fct_conversations_kpi_column as (

    {% for kpi_name in kpi_names %}
        {% for ta in time_agg%}

    select 
        '{{ ta }}' as time_agg,
        {% if ta == 'daily '%} reporting_date {% else %} date_trunc('month', reporting_date) {% endif %} as reporting_date,
        '{{ kpi_name }}' as kpi_name,
        sum({{ kpi_name }}) as kpi_value
    from fct_conversations
    group by 1,2,3
    {% if not loop.last %} union all {% endif %}
        {% endfor %}
    {% if not loop.last %} union all {% endif %}
    {% endfor %}

)

select * from fct_conversations_kpi_column
```


## 1.3. Using Macros

Macros in Jinja are pieces of code that can be reused multiple times – they are analogous to "functions" in other programming languages, and are extremely useful if you find yourself repeating code **across multiple models**. Macros are defined in `.sql` files in your `macros/` directory.

Using macros allows teams to:

- Avoid Repetition: Define common transformations or calculations once and reuse them across different models.

- Enhance Maintainability: Make global updates to logic without modifying multiple models manually.

- Improve Readability: Abstract complex logic into well-named functions, making SQL code easier to understand.

- Enable Dynamic SQL Generation: Use macros to construct queries based on variables, ensuring adaptability to different use cases.

Macros are stored in the `macros/` directory within a dbt project and can be invoked in models using Jinja syntax, such as `{{ my_macro(argument) }}`. In the following sections, we will explore examples of how macros can be used to build more modular and scalable dbt models.

> **Note:** Macros should only be used for data transformation within analytics models. Specifically, they are intended for SQL enhancement and generation, as described above. While macros can technically execute operations on tables—such as `INSERT`, `DELETE`, or `UPDATE`—these actions should not be performed through macros. Instead, such operations must be executed by dbt jobs or database administrators to maintain data integrity and avoid unintended side effects.

A basic example serving to show their use can be a function that creates the ratio of two metrics:

```sql 
{% macro create_ratio(kpi_name, value_a, value_b) %}

    (sum({{ value_a }}) / sum({{ value_b }})) as {{ kpi_name }}

{% endmacro %}
```

This function can be then applied in any model. For instance: 

```sql
with base as (

    select * 
    from {{ ref('onlyfy_fct_product_usage__xtmp_conversations_by_candidate') }}
    where reporting_date >= current_date - 30

), 

calculate_ratio as ( 

    select 
        reporting_date, 
       {{create_ratio('recruiter_reply_rate','xtmp_conversations_replied', 'xtmp_conversations_started')}} 
    from base 
    group by 1
)

select * from calculate_ratio
```

### 1.3.1. BI Macros

In our DBT environment, macros developed by BI can be found in the `macros/` folder. Below is a list of BI macros that might serve your data modelling tasks: 
  
- [group_by_cube.sql](https://github.com/new-work/dbt/blob/main/macros/group_by_cube.sql)
  - Generates a SQL query that groups by all possible combinations of a set of columns. 
  - Model example where group by cube is implemented: [xing_fct_jobs__apply_completions](https://github.com/new-work/dbt/blob/55c3e4ca503244bc287171d95ceb10e2d8eda276/models/xing/jobs/data-marts/xing_fct_jobs__apply_completions.sql#L6)

- [generatr_surrogate_key.sql](https://github.com/new-work/dbt/blob/main/macros/generate_surrogate_key.sql)
  - Generates a surrogate key column based on user defined combination of columns. This is particularly useful for incremental refresh strategies.


### 1.3.2. DBT Native Packages

A number of useful macros have also been grouped together into packages — DBT most popular package is [dbt-utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/). The dbt-utils package offers a wide range of macros. For example: 

#### 1.3.2.1. Generic Tests Macros
  
- `equal_row_count()`, asserts that two relations have the same number of rows.
    ```
    version: 2

    models:
    - name: model_name
        tests:
        - dbt_utils.equal_rowcount:
            compare_model: ref('other_table_name')
    ```

- `not_empty_string()` Asserts that a column does not have any values equal to ''.
    ```
    version: 2

        models:
        - name: model_name
            columns:
            - name: column_name
                tests:
                - dbt_utils.not_empty_string 
    ```

- `not_null_proportion()` Asserts that the proportion of non-null values present in a column is between a specified range [at_least, at_most] where at_most is an optional argument (default: 1.0).
    ```
    version: 2

    models:
    - name: my_model
        columns:
        - name: id
            tests:
            - dbt_utils.not_null_proportion:
                at_least: 0.95
    ```


#### 1.3.2.2. SQL Generators
- `pivot()` pivots values from rows to columns.
  ```
  {{ dbt_utils.pivot(<column>, <list of values>) }}

  Input: orders

  | size | color |
  |------|-------|
  | S    | red   |
  | S    | blue  |
  | S    | red   |
  | M    | red   |

  select
  size,
  {{ dbt_utils.pivot(
    'color',
    dbt_utils.get_column_values(ref('orders'), 'color')
  ) }}
  from {{ ref('orders') }}
  group by size

  Output:

  | size | red | blue |
  |------|-----|------|
  | S    | 2   | 1    |
  | M    | 1   | 0    |

  Input: orders

  | size | color | quantity |
  |------|-------|----------|
  | S    | red   | 1        |
  | S    | blue  | 2        |
  | S    | red   | 4        |
  | M    | red   | 8        |

  select
  size,
  {{ dbt_utils.pivot(
    'color',
    dbt_utils.get_column_values(ref('orders'), 'color'),
    agg='sum',
    then_value='quantity',
    prefix='pre_',
    suffix='_post'
  ) }}
  from {{ ref('orders') }}
  group by size

  Output:

  | size | pre_red_post | pre_blue_post |
  |------|--------------|---------------|
  | S    | 5            | 2             |
  | M    | 8            | 0             |
  ```

- `unpivot()`  "un-pivots" a table from wide format to long format
  ```
  {{ dbt_utils.unpivot(
  relation=ref('table_name'),
  cast_to='datatype',
  exclude=[<list of columns to exclude from unpivot>],
  remove=[<list of columns to remove>],
  field_name=<column name for field>,
  value_name=<column name for value>
  ) }}

  Input: orders

  | date       | size | color | status     |
  |------------|------|-------|------------|
  | 2017-01-01 | S    | red   | complete   |
  | 2017-03-01 | S    | red   | processing |

  {{ dbt_utils.unpivot(ref('orders'), cast_to='varchar', exclude=['date','status']) }}

  Output:

  | date       | status     | field_name | value |
  |------------|------------|------------|-------|
  | 2017-01-01 | complete   | size       | S     |
  | 2017-01-01 | complete   | color      | red   |
  | 2017-03-01 | processing | size       | S     |
  | 2017-03-01 | processing | color      | red   |
  ```

## 1.4. Let's put all together 

We will now create a table aiming to: 
- Moving conversation source values to columns
- Sum 2 metric values 
- Create metric name column 
- Aggregate by metric column and reporting date

```sql
    {% set kpi_values = [
        'xtmp_conversations_started',
        'xtmp_conversations_replied'
    ]%} -- Here we define the metrics

    {% set conversation_source_list = dbt_utils.get_column_values(
        ref('onlyfy_fct_product_usage__xtmp_conversations_by_candidate') , 'CONVERSATION_SOURCE'
        ) 
    %} -- get_column_values will extract the distinct values contained in the conversation source column

    with base as (

        {% for kpi in kpi_values%} -- start looping over the kpi_values. 
        select 
            reporting_date, 
            '{{ kpi }}' AS kpi_name,
            {{ dbt_utils.pivot (
                'CONVERSATION_SOURCE', 
                conversation_source_list,
                agg = 'sum', 
                then_value = kpi
            )
            }}
        from {{ ref('onlyfy_fct_product_usage__xtmp_conversations_by_candidate') }}
        where reporting_date >= current_date - 30
        group by 1,2
            {% if not loop.last %} union all {% endif %}
        {% endfor %}

    ), 

    clean_conversation_source_names as ( 

        select 
            reporting_date, 
            kpi_name, 
            {% for source in conversation_source_list %}
                "{{ source }}" as {{ clean_column_name(source) }}
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        from base 
    
    ), 

    final as ( 

        select * from clean_conversation_source_names
    )

    select * from final

```