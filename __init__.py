from dagster import Definitions
from .assets import web_scraped_data, mongodb_data, cleaned_data, store_to_postgres

# Define Dagster Definitions
defs = Definitions(
    assets=[web_scraped_data, mongodb_data, cleaned_data, store_to_postgres]
)
