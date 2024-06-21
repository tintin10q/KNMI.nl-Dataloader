
This is the repository for the project of the data engineering course of Radboud.

The goal of this project was to find a "bad" dataset and improve its quality. I choose [knmi.nl notification](https://developer.dataplatform.knmi.nl/notification-service) service.

I improved the dataset by creating a pipeline that listens to the incoming messages from the knmi api and stores them into a [Duckdb](https://duckdb.org/) database.
Then I created an api to download this data in the form of parquet files. 
I also created a torch dataset class and dataframe loader to interact with the api for you. 

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

The api has useful information at its `/` page.