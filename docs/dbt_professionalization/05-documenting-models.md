# Documenting dbt models

- [Documenting dbt models](#documenting-dbt-models)
  - [1. Why and what?](#1-why-and-what)
    - [Why document dbt models?](#why-document-dbt-models)
    - [General principles](#general-principles)
    - [What to document?](#what-to-document)
  - [2. How to Write Documentation in dbt](#2-how-to-write-documentation-in-dbt)
    - [a) Using the `.yml` file](#a-using-the-yml-file)
    - [b) Markdown Files](#b-markdown-files)
    - [c) Inline SQL Comments](#c-inline-sql-comments)
    - [d) README files](#d-readme-files)
  - [3. dbt Explore](#3-dbt-explore)
    - [Key Features of dbt Explore](#key-features-of-dbt-explore)


## 1. Why and what?

### Why document dbt models?

Proper documentation is essential for maintaining clarity, consistency, and usability in our dbt models. Among the benefits of documentation we can find:

- **Transparency:** Facilitates understanding of the purpose and logic behind each model.
- **Collaboration:** Makes it easier for team members to work on shared projects.
- **Troubleshooting:** Simplifies debugging and maintenance.
- **Scalability:** Ensures future team members can quickly onboard and contribute.


### General principles

When writing documentation, we shall follow these general principles:

**a) Be Clear and Concise**
- Write for your audience: Assume your reader is a colleague who has general context about the project but does not know the specifics of the model.
- Provide definitions for any domain-specific terms.

**b) Focus on Relevance**
- Highlight what is essential: the model's purpose, inputs, outputs, and key transformations.

**c) Keep It Updated**
- Documentation should reflect the current state of the model. Whenever you make changes to the model, update the corresponding documentation.


### What to document?

Ideally, each dbt model should have the following documentation elements:

**a) Model Description**
- **Purpose:** Why does this model exist? What question or problem does it solve?
- **Key Metrics or Outputs:** Describe the primary outputs of the model and how they are used.
- **Stakeholders:** Identify who relies on this model (teams or individuals).
- **External References:** Link any Jira ticket or Confluence page with necessary context for the model.

**b) Transformations**
- Provide a high-level overview of the key transformations applied in the model (e.g., joins, aggregations, filtering logic).
- Mention any significant assumptions or business logic embedded in the transformations.

**c) Testing and Validation**
- Specify the tests applied to the model (e.g., uniqueness, not null, relationships).
- If applicable, note any edge cases or potential limitations of the model.

**d) Usage Notes**
- Indicate how and where this model is used (e.g., dashboards, reports, downstream models).
- Provide any important caveats or considerations when using the model.


## 2. How to Write Documentation in dbt

Documentation in dbt can and should be written in several locations:

### a) Using the `.yml` file
- Add documentation directly within the directory's `.yml` file for the model's and column's descriptions.
- Example:
  ```yaml
  models:
    - name: central_dim_job_postings
      description: | 
        * Dimension table holding job posting information.
        * Stakeholders: All Analytics teams.
      columns:
        - name: job_posting_id
          description: Primary key of job posting
          data_tests:
            - unique
            - not_null
        - name: created_at
          description: Timestamp of the creation of the job posting
          data_tests:
            - not_null
        - name: expired_at
          description: Timestamp of the expiration of the job posting
        - name: activated_at
          description: The date when the job posting was activated.
        - name: deactivated_at
          description: Indicates the date when the job posting was deactivated
        .
        .
        .
  ```

### b) Markdown Files
- Use markdown files for more detailed or project-level documentation (e.g., including images, diagramas or links). Description blocks are wrapped by Jinja `docs` delimiters. 
- Example:
    ```markdown
    {% docs xing_int_jobs__high_revenue_postings_docs %}

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

    - `service_offering_group_id`: The ID of the service offering group which decides about the type of
    boosting and other premium (_e.g._ Top4Top settings) that we offer for the job posting. [Specific information
    can be found here](https://new-work.atlassian.net/wiki/spaces/JOBSPLAT/pages/39359934/Jobs+Service+Offering+Groups).

    | Service offering group | Onlyfy products        |
    | ---------------------- | ---------------------- |
    | 0                      | Old portfolio, Core360 |
    | 1                      | Core                   |
    | 2                      | Pro                    |
    | 3                      | Ultimate               |
    | 4                      | Essential              |
    | 5                      | Core15                 |

    ## Dependencies

    The model depends mainly on the central postings table and (Onlyfy) salesforce tables.

    ## dbt Jobs and Automations

    - **dbt Job**: [Link](https://emea.dbt.com/deploy/74/projects/309/jobs/90428)
    - **Airflow DAG**: [xing_jobs__int_job_models](https://airflow-bi.xing.io/dags/xing_jobs__int_job_models/grid)

    {% enddocs %}
    ```

- Then use the code block above within the `.yml` file for the model's description:
  ```yaml
  models:
    - name: xing_int_jobs__high_revenue_postings
      description: '{{ doc("xing_int_jobs__high_revenue_postings_docs") }}'
  ```
- You can follow the same logic to describe columns and re-utilize column descriptions that are repeated across models.
- **Note:** `docs` variables apply to the entire dbt project. It is therefore advisable to follow naming conventions to make them exclusive to your own domain.

### c) Inline SQL Comments
- Use inline comments (`--`) in SQL files for specific transformations or complex logic.
- Example:
    ```sql
    select * 
    from {{ ref('central_fct_adobe_tracking_events') }}
    where true
        and created_at::date between '2022-01-01' and current_date - 1
        and (  lower(site_section) like '%stellenmarkt%'     -- original Jobs pages
            or lower(site_section) like '%/jobs%'            -- '/jobs' will replace 'stellenmarkt'
            or (created_at::date >= '2024-07-01'             -- Starting July 2024,
                and pagename = 'wbm/Welcome/start/index'     --  the startpage must be considered a Jobs page
                and login_status = 'Logged Out'              --  for Logged-out
            )
        )
    ```

### d) README files
- `README.md` files are Markdown files that are automatically rendered on GitHub.com when placed in a directory. This makes them easy to read and accessible directly from the web.
- These files are particularly useful for general documentation that applies to several dbt models or for documenting projects involving multiple dbt models. They provide flexibility to include detailed narratives, diagrams, and external references.
- Example:
    ```markdown
    # Conversational Search (CS) AB Tests

    ### ABACUS-439

    First AB test where the Beta banner was intriduced for the test group. It was rolled-out to 100% right after to achieve more traffic.

    * Web, Logged-In.
    * Jira ticket [link](https://new-work.atlassian.net/browse/ABACUS-439)

    ### ABACUS-443

    Similar to ABACUS-439 but for Logged-Out. Data models are the same.

    * Web, Logged-Out.
    * Jira ticket [link](https://new-work.atlassian.net/browse/ABACUS-443)

    ### ABACUS-468

    New CS experiment where the CS bar replaces the old one on Startpage.

    * Web, Logged-In.
    * Jira ticket [link](https://new-work.atlassian.net/browse/ABACUS-468)

    ### ABACUS-455

    CS experiment on Native (similar setup as in ABACUS-468).

    * Android and iOS.
    * A: ES (20%) | B: CS (20%) | C: CS on banner (60%)
    * Jira ticket [link](https://new-work.atlassian.net/browse/ABACUS-455)

    ```

## 3. dbt Explore

The **dbt Explore** is a powerful addition introduced by dbt Cloud. It allows users to explore and understand dbt models directly from the cloud interface, providing insights into how data flows through analytics environment.<br>
You can access dbt Explore [here](https://emea.dbt.com/explore/74/projects/309/environments/production/)

### Key Features of dbt Explore

#### Integration with Documentation <!-- omit from toc -->

You can easily link to and view model documentation that you've written as part of your dbt project.

#### Detailed Metadata <!-- omit from toc -->

dbt Explore provides a quick way to access details like the columns in a model, data types, descriptions, relationship with other models etc.

#### Detailed Information about Tests <!-- omit from toc -->

dbt Explore includes metadata about tests associated with the models (e.g., uniqueness, not null, or custom tests defined in the schema YAML) and whether these tests are passing or failing.<br>
It is also possible to click into individual tests for more details, such as the query generated for the test and why it failed.

#### Visual Exploration of Lineage <!-- omit from toc -->

You can view upstream and downstream dependencies for a particular dbt model, snapshot, or source.<br>
This helps in understanding the context of your data and how changes might impact other parts of the analytics pipeline.

#### Column Lineage <!-- omit from toc -->

Explore can also show column-level lineage.<br>
This enables even more granular tracking of where specific fields originate and how they transform across your models.

#### Easy Access to Code <!-- omit from toc -->

You can easily view both raw and compiled SQL for a model.<br>
Compiled SQL is the result of dbt's parsing and rendering of the Jinja-templated SQL with the actual configurations, macros, and parameters applied.

#### Performance Information <!-- omit from toc -->

In dbt Explore, you can view performance metrics related to model executions.

- **Model-Job Relationships:** You can see which jobs trigger a specific model. This is useful for understanding the operational context of the model within your analytics workflow.<br>
  For example, if a model is included in multiple jobs (e.g., nightly runs, ad hoc jobs, or specific environments like dev/prod), Explore lists those jobs.
- **Run History:** You can access past execution logs that show:
  - Execution time for the model.
  - The number of rows processed.
  - Warnings or errors during execution.
- **Timing Breakdown:** dbt Cloud also provides granular timing information for:
  - Compilation time.
  - Execution time in the data warehouse.
  - Macro resolution time, etc.

- **Consumption Queries:** Integrated with Snowflake logs, dbt Cloud shows how many times a specific model's table or view is queried by downstream consumers.

#### Interactivity <!-- omit from toc -->

Users can click through the lineage graph to navigate between related models, sources, and other artifacts in the pipeline.

#### Faster Debugging <!-- omit from toc -->

If you're troubleshooting issues or trying to determine how a model is being used, Explore simplifies the process by centralizing lineage and metadata into an accessible view.
