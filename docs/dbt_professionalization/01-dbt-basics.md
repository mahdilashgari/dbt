# Development with dbt

- [Development with dbt](#development-with-dbt)
  - [Test and Production Environments](#test-and-production-environments)
    - [Test Environment:](#test-environment)
    - [Production Environment:](#production-environment)
    - [The `defer` function](#the-defer-function)
  - [Built-in Generic Tests](#built-in-generic-tests)
      - [Built-in Generic Tests](#built-in-generic-tests-1)


## Test and Production Environments

In dbt, it's crucial to manage separate environments to ensure the quality and reliability of your data models. In our current setup, we work in two key environments: **Test** and **Production**.


### Test Environment:

The test environment is a safe space for both data engineers and analysts to work on new features, modifications, or bug fixes. It mirrors the production environment but operates with isolated data. Changes should always be tested here first before being pushed to production.

The test enviroment uses a git branching strategy: different branches are created for different features or modifications, ensuring that each set of changes is isolated. Once the code is validated in the test environment, it can be merged into the `main` branch and later deployed to production. This branching strategy ensures that development work remains organized and can be easily reversed if issues arise.

Developers can experiment in the test environment without risking production data. Likewise, new data models will be materialized in your own development schema: `TEST_NAME_SURNAME`.

There are several key scenarios where the Test environment is particularly valuable. For instance:

* It can be used to **compare changes to the current Production tables**, providing a clear view of how modifications will impact live data. To accomplish this, materialize test models in your development schema and use SQL queries or Tableau to compare results from both environments side-by-side. Utilizing the `defer` function with dbt build is also helpful, as it allows dbt to refer to existing Production versions of upstream models without needing to rebuild them in the Test environment. See [The `defer` function](#the-defer-function) below.

* The Test environment is also essential for **giving stakeholders a preview of changes**. Develop the model in your Test environment and share numbers or visualizations that display the new data output.

* Additionally, the Test environment can facilitate the **preview of changes in Tableau**. By configuring a separate data source in Tableau that points to the Test environment, you can safely review how your data models affect downstream reports and dashboards. **Note:** BI will soon allow for a test space in Tableau.

* The Test environment is particularly useful for **validating incremental models or complex pipelines** with numerous dependencies. This approach lets you verify the model with a subset of data and catch potential issues before they reach Production.

* Other scenarios include **testing new data sources or integrations and optimizing performance**. In both cases, you can utilize the Test environment to test the pipeline and monitor improvements.

By following these best practices and leveraging dbt commands effectively, the Test environment enables you to validate changes, gather stakeholder feedback, and confidently prepare your models for Production.


### Production Environment:

The production environment is where your dbt models are executed in the final, live setting. This environment is stable, and any code changes should be thoroughly tested and validated before deployment to avoid disruptions.

The `main` branch from the GitHub repository is reserved for models in the Production environment. Changes to production models are then merged into `main` by a pull request after passing the necessary testing and validation procedures.

Finally, models in the production environment are materialized by configuring and running **dbt Cloud Jobs**. The resulting tables are created in one of the production Analytics schemas:

- CENTRAL
- ONLYFY
- XING
- XMS


### The `defer` function
[`defer` dbt docs](https://docs.getdbt.com/docs/cloud/about-cloud-develop-defer)

The defer function in dbt is used to optimize development workflows by allowing developers to run models in a test environment without rebuilding upstream models, instead relying on the production versions of those upstream models. This is particularly useful when working with large datasets or when you want to avoid rerunning time-consuming models during development and testing.

When you run dbt with the `--defer` flag or when `defer` is enabled, dbt will:

- Check if any models that your current models depend on already exist in production.
- Instead of rebuilding these upstream models from scratch in your test environment, dbt will defer to the existing materialized models in the production environment.

Notice that if a development (test) version of a deferred model exists in your test schema, dbt will give preference to the test model over the production model.

Deferring to production saves time and resources by leveraging already built models, allowing you to focus on the changes you are actively developing and testing.

As a best practice, it's recommended to drop all objects in the development schema at the start and end of your development cycle to maintain a clean working environment.

```md
—— In detail ——

When `defer` is enabled, the `ref()` function is resolved to a full database object name that depends on the
existence of object in development schema. E.g., `ref('central_dim_calendar')` will be resolved as:

- `ANALYTICS.TEST_NAME_SURNAME.DIM_CALENDAR` if the model exists in the development schema.
- `ANALYTICS.CENTRAL.DIM_CALENDAR` if the model doesn't exist in development schema.
```


## Built-in Generic Tests
[Generic data test — dbt docs](https://docs.getdbt.com/docs/build/data-tests#generic-data-tests)

When developing a model, ensuring the accuracy and integrity of your data is critical. **Generic tests** help catch potential issues early in the development process, preventing errors from propagating to production. By applying tests like `not_null` and `unique`, you can validate key assumptions about your data, such as ensuring primary keys are unique or that mandatory fields are populated. This helps avoid costly errors, improves trust in your models, and accelerates the development process by automating key validation steps. Integrating generic tests from the start ensures that the model behaves as expected before being deployed.


#### Built-in Generic Tests

dbt provides several built-in generic tests, including:

- **`unique`**: Ensures that all values in a column are unique. This is critical for **primary keys** or columns where duplicate values would cause issues (*e.g.*, user IDs, transaction IDs). We should always apply it to these columns or any other column that must not contain duplicates.
- **`not_null`**: Ensures that no null values exist in a column. This helps in ensuring that critical columns contain valid data, avoiding issues with incomplete records. We should always apply the `not_null` test on primary keys, foreign keys, and any other critical fields that should not have missing values.
- **`accepted_values`**: Verifies that column values belong to a specified set of values. It ensures data consistency by validating that values conform to expected categories or types. This is particularly useful for categorical fields like status codes, categories, or other controlled vocabularies. We would apply `accepted_values` tests on columns with a limited set of valid values or when `case when` statements are used to classify values into categories.
- **`relationships`**: Checks referential integrity between two tables (*i.e.*, a foreign key constraint). This test guarantees that all records in a table have valid references to related records in another table (*e.g.*, orders linked to existing users). We would use the relationships test for any foreign key where relationships are critical.

You can apply generic tests directly in your model’s schema YAML file by specifying the test and the column you want to validate. For example:

```yaml
models:
  - name: central_fct_adobe_tracking_events
    columns:
      - name: xing_user_id
        data_tests:
          - unique
          - not_null
      - name: login_status
        data_tests:
          - accepted_values:
              values: ['Logged Out', 'Logged In']
          - relationships:
              to: ref('central_stg_core__login_status')
              field: login_status
```

To run all the tests in your project, use: `dbt test`. This will execute both built-in and custom generic tests and report any failures, allowing you to quickly identify data quality issues.
