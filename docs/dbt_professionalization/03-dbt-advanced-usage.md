# dbt Advanced Usage

- [dbt Advanced Usage](#dbt-advanced-usage)
  - [Visual Studio Code](#visual-studio-code)
  - [dbt Cloud CLI](#dbt-cloud-cli)
    - [Installation guide](#installation-guide)
    - [VS Code Extensions](#vs-code-extensions)
    - [VS Code Usage Tips](#vs-code-usage-tips)
  - [Github Copilot in VS Code - Quick Tutorial](#github-copilot-in-vs-code---quick-tutorial)
    - [Tutorial goal](#tutorial-goal)
    - [What is GitHub Copilot](#what-is-github-copilot)
    - [Why GitHub Copilot?](#why-github-copilot)
    - [Copilot Features](#copilot-features)
      - [Chat Participants](#chat-participants)
      - [Slash Commands](#slash-commands)
      - [Chat variables](#chat-variables)
      - [Pull request summaries](#pull-request-summaries)

By following this documentation, you will be able to use the dbt Cloud CLI within Visual Studio Code (VS Code).<br>
Using VS Code for dbt development provides several advantages over the dbt Cloud IDE:
* **Faster Development:** With local access to files, you can make and save changes more quickly compared to dbt Cloud IDE
* **Performance:** Generally faster and more lightweight
* **Extension Ecosystem:** Extensive marketplace for various extensions (GitHub Copilot, GitLens, many other scripting or highlighting extensions)
* **Multi-File Management:** Supports efficient navigation and management of multiple files and folders
* **Customizable environment:** Configure VS Code with plugins, settings, and themes that suit your workflow, helping you optimize productivity

## Visual Studio Code

1. Install VS Code from the Software Center.
2. Clone the [new-work/dbt](https://github.com/new-work/dbt) repository in a dedicated local directory and open it in VS Code.

## dbt Cloud CLI

The dbt Cloud CLI allows you to run dbt commands against your dbt Cloud development environment from your local command line.<br>

### Installation guide

⚠️ Some parts of the following guide is prepared with MacOS based details. Commands or directories might be different for Windows users.

Before starting; keep in mind that if you have dbt-core installed locally, the cloud CLI will conflict.<br>
You can verify if you already have dbt Core installed by running the following command:<br>
`which dbt`

If the output is `dbt not found`, then that confirms you don't have it installed.<br>
If you've installed dbt Core globally in some other way, uninstall it first before proceeding:<br>
`pip uninstall dbt`

3. Follow the installation documentation provided by dbt. Note that documentation has different tabs for macOS and Windows installations.<br>
   [Install dbt Cloud CLI](https://docs.getdbt.com/docs/cloud/cloud-cli-installation)<br>
   Note that;
    - Step 4 inside dbt documentation is about cloning repository to local computer, which is already done in previous step in this page.
    - Step 5 inside dbt documentation is about configuring the dbt Cloud CLI, which redirects you to the documentation mentioned in following step in this page.

4. Follow the configuration documentation provided by dbt.<br>
   [Configure and use the dbt Cloud CLI](https://docs.getdbt.com/docs/cloud/configure-cloud-cli)<br>

   This step can be summarized in two steps:
    - Download CLI configuration file from https://emea.dbt.com/settings/profile/cloud-cli
    - Save it into proper directory on your local:
      - Mac or Linux: `~/.dbt/dbt_cloud.yml`
      - Windows: `C:\Users\yourusername\.dbt\dbt_cloud.yml`

   Note that;
    - Config file (`dbt_cloud.yml`) you downloaded will look like the following:
      ```bash
      version: "1"
      context:
      active-host: "emea.dbt.com"
      active-project: "309"
      projects:
      - project-name: "Analytics"
         project-id: "309"
         account-name: "New Work SE"
         account-id: "74"
         account-host: "emea.dbt.com"
         token-name: "<pat-or-service-token-name>"
         token-value: "<pat-or-service-token-value>"
      ```
    - Step 4 inside dbt documentation is **not** mandatory, please skip it.
    - We don't need to set environment variables or do additional configurations assuming credentials on dbt Cloud are already set correctly, please skip it.

   You can create a quick access to `dbt_cloud.yml` file within the dbt folder by typing in the VS Code terminal the following command:

   ```bash
   #Make sure you are in your dbt folder while running this command.
   ln -s '~/.dbt/dbt_cloud.yml' dbt_cloud.yml
   ```

5. **defer function:** `--defer` is automatically enabled in the dbt Cloud CLI for all invocations, you don't need to do any configurations. See [documentation](https://docs.getdbt.com/docs/cloud/about-cloud-develop-defer#defer-in-dbt-cloud-cli)<br>
   You can disable it with the `--no-defer` flag.

6. **SQLFluff:** You can invoke SQLFluff from the dbt Cloud CLI without installing any packages. See [documentation](https://docs.getdbt.com/docs/cloud/configure-cloud-cli#lint-sql-files)<br>
   However, VS Code extensions expects SQLFluff to be installed.

7. You are finally ready to test VS Code: open any lightweight model and type `dbt build --select <model_name>` into your terminal.



### VS Code Extensions
Inside VS Code, go to the **Extensions** panel and add the following:
- [Power User for dbt](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user)<br>
   - Auto-fill model names, macros, sources and docs
   - Click on model names, macros, sources to go to definitions
   - View model lineage as well as column lineage
   - ⚠️ This extension comes with additional features like compiling/building/testing models or running the model and previewing the results etc.<br>
      However, these features require an API based integration with the developer company, this will be investigated from data protection perspective.
      If there is no violation, these features can be enabled. Until then please avoid using these features.
- [Sqlfluff](https://marketplace.visualstudio.com/items?itemName=dorzey.vscode-sqlfluff)
   - A linter and auto-formatter for SQLFluff, a popular linting tool for SQL and dbt
   - This extension expects sqlfluff to be installed
   - It smoothly works without any specific configurations, but plese feel free to contact if you have issues

### VS Code Keyboard Shortcuts

Please find below some vs code keyboard shortcuts which could save some time in your daily workflow.

VS Code Editor
* Command Palette (to look for vs code functionalities): Ctrl + Shift + P (Windows/Linux), Cmd + Shift + P (macOS)
* Open File: Ctrl + P (Windows/Linux), Cmd + P (macOS)
* New File: Ctrl + N (Windows/Linux), Cmd + N (macOS)

Navigation
* Toggle Sidebar: Ctrl + B (Windows/Linux), Cmd + B (macOS)
* Navigate Between Open Editors: Ctrl + Tab (Windows/Linux), Cmd + Option + Left/Right Arrow (macOS)
* Search in Files: Ctrl + Shift + F (Windows/Linux), Cmd + Shift + F (macOS)

Copilot 
* Open chat: Ctrl + i (Windows/Linux), Cmd + cntrl + i (macOS)
* Open suggestions: Ctrl + enter(Windows/Linux/macOS)

### VS Code Usage Tips
- Use standard dbt commands within the vscode terminal.
- Always ensure you're on the right branch before making modifications.
<br><br>


## Github Copilot in VS Code - Quick Tutorial

### Tutorial goal 

Understand what GitHub Copilot is, its features and how they can support your daily work.

### What is GitHub Copilot

"GitHub Copilot is an **AI coding assistant** that **helps you write code faster and with less effort**, allowing you **to focus more energy on problem solving** and collaboration." 
([Source GitHub](https://docs.github.com/en/copilot/about-github-copilot/what-is-github-copilot)). 

GitHub Copilot generates suggestions using probabilistic determination. It generates code suggestions by looking at your code space, open editor, file paths, URLs of your repository. 

### Why GitHub Copilot? 

Copilot uses **Open AI's Codex Model**, which focues its training on a wide range of programming languages and coding contexts. 
**This makes Copilot a specialised and knowledgeable programming language assistant. 

Below you can find results of an experiment conducted by GitHub, comparing developers performance w/ and w/o Copilot.

<img src="../files/copilot_img1.png" alt="alt text" width="500" height="450">

Results were statistically significant (P=.0017) and 95% confidence interval for percentage speed gain is [21%, 89%].
Please visit this [resource](https://github.blog/news-insights/research/research-quantifying-github-copilots-impact-on-developer-productivity-and-happiness/) for more details.

### Copilot Features

- Code complition: get code suggestions as you develop in your IDE
- Copilot chat: ask for help to fix, improve and develop your code
- Copilot pull requst summaries: create pull request summary comments

#### Chat Participants

AI Domain experts that will support you in specific domains. 

- @workspace
- @vscode
- @terminal

#### Slash Commands 

Use these commands to avoid typing long prompts. 

- /fix: to find issues and suggest fixes in your code
- /explain: to explain what the code does
- /doc: generates code documentation

More commands can be found [here](https://code.visualstudio.com/docs/copilot/copilot-chat#_slash-commands).

#### Chat variables

Use chat variables to add context to your prompt. 

- #file: include a specific file as context in the chat
- #terminalLastCommand: Include the last run command in the active Visual Studio Code terminal.

More variables can be found [here](https://code.visualstudio.com/docs/copilot/copilot-chat#_chat-variables).

#### Pull request summaries and commit messages

Copilot additionally suports you in creating pull request summaries and commit messages. 

In VS Code you can prompt Copilot to generate a commit message based on the incoming changes. 