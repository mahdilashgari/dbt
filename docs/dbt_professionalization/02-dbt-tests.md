# Advanced Testing

- [Advanced Testing](#advanced-testing)
  - [Intro to data tests](#intro-to-data-tests)
  - [Singular tests](#singular-tests)
  - [Generic Tests](#generic-tests)
    - [Built-in generic tests](#built-in-generic-tests)
    - [More generic tests](#more-generic-tests)
  - [Data test configurations](#data-test-configurations)
    - [`limit`](#limit)
    - [`where`](#where)
    - [`severity`](#severity)


## Intro to data tests

Data tests in dbt let you create assertions about your models and other project components, such as sources, seeds, and snapshots. When running `dbt test`, dbt verifies whether each of these assertions in the project passes or fails.

By using data tests, you can enhance the reliability of the SQL in each model by setting expectations for the generated results. Out of the box, dbt includes the **built-in generic tests** for common conditions: for example, ensuring that a column only contains non-null values, unique values, or values that are correctly linked between models (e.g., confirming that an order table’s customer_id corresponds with an id in a customers model). See [Built-in Generic Tests](./01-dbt-basics.md#built-in-generic-tests). You can also expand tests to match specific business rules — any assertion that can be written as a select query can serve as a data test.

Data tests flag any records that don't meet the defined criteria. Data tests in dbt are actually SQL queries — select statements that look for "failing" records. For example, if you assert that a column must be unique, the test will select duplicates; if you assert no null values, the test will return any nulls it finds. A data test passes if it returns zero failing rows, confirming your assertion.

There are two main ways to define data tests in dbt:

- **Singular data tests** are straightforward queries that locate failing rows and are saved in `.sql` files within the test directory. When running `dbt test`, these queries are executed as data tests.

- **Generic data tests** are reusable parameterized queries. Once defined in a test block, they accept parameters and can be referenced by name in `.yml` files to test across models, columns, sources, snapshots, and seeds. dbt comes with four built-in generic data tests that cover common assertions.

Defining data tests is an effective way to ensure that inputs and outputs meet expectations and to prevent regressions as code changes. Generic tests are particularly versatile, allowing similar checks with slight variations, so they often form the core of a dbt testing suite. Both types of data tests have unique uses and add value to the testing process.


## Singular tests

A singular data test is the most straightforward form of testing in dbt. If you can write a SQL query that returns rows for failing conditions, simply save that query as a `.sql` file in your `tests/` directory, and it becomes a data test for a specific purpose.

This approach involves directly writing SQL to identify failing records, hence the name “singular” data tests — they’re unique, one-time checks for particular cases. Each file contains a single select statement, defining one specific test, like:

```sql
-- This model stores top-paid postings created (a high-level KPI) and serves several teams.
-- Return records where n_postings drops below 100 to make the test fail.
select
    created_at::date       as date_id,
    count(job_posting_id)  as n_postings
from {{ ref('xing_int_jobs__high_revenue_postings') }}
where created_at::date = current_date-1
group by 1
having n_postings < 100
```

As seen in the example, within test files you can use Jinja, along with `ref` and `source` functions, just as you would in models. This ensures that once your model is built, your tests are run in the appropriate dependency order.

Since singular data tests are so easy to set up, you may find yourself writing similar test structures repeatedly, simply changing the column or model name. When that happens, it’s worth considering generic data tests to make reusable assertions across multiple models.


## Generic Tests

Some data tests in dbt are "generic", meaning they can be reused in multiple places across different models. A generic data test is created within a test block that includes a parameterized query and accepts arguments — because of this, they are called **custom generic tests**. Here’s an example:

```sql
-- tests/generic/count_min_events.sql
-- Return records where n_column drops below min_value to make the test fail.
{% test count_min_events(model, column_name, min_value) %}
select
    created_at::date            as date_id,
    count( {{ column_name }} )  as n_column
from {{ model }}
where created_at::date = current_date-1
group by 1
having n_column < {{ min_value }}
{% endtest %}
```

In this example, the test accepts three parameters — `model`, `column_name` and `min_value`. These parameters allow flexibility, enabling you to set specific conditions for different columns or models. You can apply this test to any column by passing the relevant arguments in your `.yml` files. dbt will substitute the values accordingly when the test is run. Once defined, you can add the custom generic test as a property on any model, source, seed, or snapshot.

Here’s how to configure it in a `.yml` file:

```yml
version: 2
models:
  - name: xing_int_jobs__high_revenue_postings
    columns:
      - name: job_posting_id
        data_tests:
          - count_min_events:
              min_value: 100
```

Similar to singular tests, generic tests are saved into the tests folder, however in a dedicated subfolder: `tests/generic/`. Remember that generic tests are model-independent, however you can to create a domain-specific folder in this directory —such as `tests/generic/xing`— for a better organization when your test is particularly domain-dependent.

Feel free to explore further on this and on custom generic tests in dbt's official documentation: [Writing custom generic tests](https://docs.getdbt.com/best-practices/writing-custom-generic-tests).


### Built-in generic tests

dbt ships with four generic data tests already defined: **unique**, **not_null**, **accepted_values** and **relationships**. You can take a look at them in [Built-in Generic Tests](./01-dbt-basics.md#built-in-generic-tests).


### More generic tests
Various open-source packages offer additional generic tests that can enhance the functionality of your dbt models. These packages are available for import, as long as their usefulness and reliability have been considered. In fact, the most popular and widely used packages, such as **dbt_utils** and **dbt_expectations** are already pre-installed in this project:

- [dbt_utils package](https://github.com/dbt-labs/dbt-utils/tree/main?tab=readme-ov-file#generic-tests) offers some generic tests out of the box:

  - **recency:** Asserts that a timestamp column in the reference model contains data that is at least as recent as the defined date interval.
  - **not_accepted_values:** Asserts that there are no rows that match the given values.
  - **equal_rowcount:** Asserts that two relations have the same number of rows.
  - **expression_is_true:** Asserts that a valid SQL expression is true for all records. This is useful when checking integrity across columns.
  - **not_null_proportion:** Asserts that the proportion of non-null values present in a column is between a specified range
  - **mutually_exclusive_ranges:** Asserts that for a given lower_bound_column and upper_bound_column, the ranges between the lower and upper bounds do not overlap with the ranges of another row.

  ```yml
  - name: central_dim_salesforce_accounts
    config:
      tags:
        - pii
    description: "Table with Salesforce Accounts."
    columns:
      .
      .
      .
      - name: duns_number
        description: "DUNS number of the account."
        data_tests:
          - dbt_utils.not_null_proportion:
              at_least: 0.85
  ```

- [dbt_expectations package](https://github.com/calogica/dbt-expectations/tree/main?tab=readme-ov-file#available-tests) offers as well some useful tests, _e.g._:

  - **expect_column_values_to_be_between:** Expect each column value to be between two values.
  - **expect_column_values_to_match_like_pattern:** Expect column entries to be strings that match a given SQL like pattern.
  - **expect_column_most_common_value_to_be_in_set:** Expect the most common value to be within the designated value set
  - a lot more tests including statistical assertions

  ```yml
  - name: central_dim_salesforce_accounts
    config:
      tags:
        - pii
    description: "Table with Salesforce Accounts."
    columns:
      .
      .
      .
      - name: billing_country
        description: "Country of the billing address."
        data_tests:
          - dbt_expectations.expect_column_most_common_value_to_be_in_set:
              value_set: ["Germany"]
              top_n: 1
  ```

Refer to the packages documentation for full lists of available tests and configuration options, and see the docs on packages for details on adding these and other external dbt packages.


## Data test configurations

In dbt, data test configurations offer flexibility in managing how tests are executed and how results are handled, allowing you to fine-tune tests to better fit your project’s needs. Configurations like **limit**, **where**, and **severity** enable you to control the volume of failing rows returned, specify conditional filters, and set response levels for test results. By using these options, you can ensure that tests focus on relevant data subsets and appropriately categorize issues as warnings or errors, giving teams better control over data quality and validation processes.

For more configurations please refer to [Data test configs](https://docs.getdbt.com/reference/data-test-configs).


### `limit`

This configuration *limits* the number of failing rows returned by a test. To use it, we set a maximum number of rows the test should return in case of failure. For example, we can set a limit of 100 to restrict the output to the first 100 failing rows:

```yml
models:
  - name: central_fct_adobe_tracking_events
    columns:
      - name: login_status
        data_tests:
          - accepted_values:
              values: ['Logged Out', 'Logged In']
              config:
                limit: 100
```


### `where`

This configuration adds a conditional filter to the test to target specific records or cases within the data. In particularm it uses a where clause to specify conditions, similar to SQL, allowing the test to run only on rows that meet certain criteria. For example, applying a test only to web:

```yml
models:
  - name: central_fct_adobe_tracking_events
    columns:
      - name: login_status
        data_tests:
          - accepted_values:
              values: ['Logged Out', 'Logged In']
              config:
                where: "activity_platform ilike 'web%'"
```


### `severity`

You can configure tests in dbt to trigger **warnings** instead of **errors** or set them to respond conditionally based on the number of failed records. For instance, **you might want just one duplicate record to show as a warning, but ten duplicate records to raise an error**.

The configurations available under this type include:

- `severity`: Can be set to `error` or `warn` (default: `error`).
- `error_if`: A condition (default: `!= 0`) specifying when the test should return an error.
- `warn_if`: A condition (default: `!= 0`) specifying when the test should return a warning.

These conditional expressions use standard SQL comparison logic with the count of failed records (e.g., `> 5`, `= 0`, `between 5 and 10`).

Here’s how dbt evaluates these settings:

- If severity is set to `error`, dbt first evaluates the `error_if` condition. If it’s met, the test returns an error. If not, dbt checks the `warn_if` condition (by default, `!= 0`). If `warn_if` is met, the test issues a warning; if not, it passes.
- If severity is set to `warn`, dbt skips `error_if` and goes directly to `warn_if`. If `warn_if` is met, the test returns a warning; otherwise, it passes.

In the following example, we raise an error if the number of failing records is larger than 100 (as in the `limit` case) but we now set a warning if the number of failing records is larger than 20:

```yml
models:
  - name: central_fct_adobe_tracking_events
    columns:
      - name: login_status
        data_tests:
          - accepted_values:
              values: ['Logged Out', 'Logged In']
              config:
                severity: error
                error_if: ">100"
                warn_if: ">20"
```

Additionaly, if the `--warn-error` flag is used, warnings will be treated as errors. Without this flag, any test configured with `warn` severity will not escalate to an error.
