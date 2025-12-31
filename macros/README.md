# Macros

Macros in Jinja are pieces of code that can be reused multiple times – they are analogous to "functions" in other programming languages, and are extremely useful if you find yourself repeating code across multiple models.<br>
More information about macros can be found [here](https://docs.getdbt.com/docs/build/jinja-macros)

The macros are classified as the following:

- **utils:** reusable code snippets implemented as functions and can be called within models
- **analytics:** developed by analytic teams - individual macros can be moved into utils folder in case of a generic usability
- **governance:** used for data governance related processes
- **raw_layer:** used for transformation of raw layer
- **snapshot_layer:** used for transformation of snapshot layer
- **tech:** used to perform pure technical operations such as housekeeping, maintenance, database customizations etc.
- **central:** used specifically during implementation of certain models - not a common use case
