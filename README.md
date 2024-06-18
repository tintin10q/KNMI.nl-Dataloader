
This is the repository for the project of the data engineering course of Radboud.

Install the dependencies with: 

```shell
poetry install
```

Run the pipline with

```shell
poetry run listen-nc
```

Run the api with

```shell
poetry run start-api
```

This will create the database, config.toml and auth.toml if it does not exist yet.

If you are running the pipeline make sure to fill in the following things in the auth.toml:

```toml

# Client ID should be made static, it is used to identify your session, so that missed events can be replayed after a disconnect
CLIENT_ID = "........-....-....-....-............"
# Obtain your token at: https://developer.dataplatform.knmi.nl/notification-service
TOKEN = "ey..."
FILE_DOWNLOAD_TOKEN = "ey..."
```
