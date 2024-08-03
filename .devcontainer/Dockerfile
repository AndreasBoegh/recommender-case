FROM mcr.microsoft.com/devcontainers/python:1-3.9-bullseye
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8

ENV JAVA_HOME /usr/local/openjdk-8

USER vscode

# Install databricks-cli
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

RUN pipx install poetry

RUN sudo apt update && sudo apt -y install xdg-utils

RUN poetry init -n --directory=/home/vscode \
  && cd /home/vscode \
  && poetry add pandas==2.0.0 \
  && poetry add pyspark==3.5.1 \
  && poetry install

