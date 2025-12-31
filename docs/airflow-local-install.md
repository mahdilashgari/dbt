# Local Installation of Airflow for DAG Development

Sometimes DAG development can be a bit slow when you have to wait for the CI/CD pipeline to run after PR. To speed up the development process, you can install Airflow locally on your machine.
Astro is a tool that makes it easy to run Apache Airflow locally. It provides a simple CLI to start, stop, and manage Airflow services.

## Mac

To install Astro on Mac for DAG development, follow these steps:

1. Open a terminal window.

2. Install Homebrew by running the following command:
   ```shell
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ```
    More info: https://brew.sh/
3. Install `docker` by running the following command:
    ```shell
    brew install --cask docker
    ```
4. Install the `astro` CLI by running the following command:
    ```shell
    brew install astro
    ```
5. Verify the installation by running the following command:
    ```shell
    astro version
    ```

## Windows

1. Install winget
2. Install Docker Desktop
    ```shell
    winget install -e --id Docker.DockerDesktop
    ```
3. Install Astro
    ```shell
    winget install -e --id OpenAstro/astro
    ```

# Running astro

1. (First time only) Initiate your local project folder by running:
    ```shell
    astro dev init
    ```
2. Start the Astro server by running:
    ```shell
    astro dev start
    ```
3. Open your browser and navigate to `http://localhost:8080` to access the Astro UI.
4. Develop your DAGs in the `dags` folder.
5. When you are done, stop the Astro server by running:
    ```shell
    astro dev stop
    ```

# Configuring your project

1. Required pyhton packages can be defined in the `requirements.txt` file.
2. The `airflow_settings.yaml` file can be created to define Airflow connections.
You can find example to copy from 1Password [here](https://start.1password.com/open/i?a=FQZ6XKNENJH23GWC3L6E7QQEKQ&v=e3goxqp77uwefhy2jk324vojyi&i=6pkr537yvxz7eh2jmcvhspha3y&h=newworkse.1password.eu).
