from databricks.sdk.runtime import *

from Config.secrets_config import bronze_storage_key, input_storage_key
from Config.storage_config import storage_account, storage_input

# 1. Setando configuração de acesso utilizando a key
def apply_storage_config(spark):
    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        bronze_storage_key
    )
    spark.conf.set(
        f"fs.azure.account.key.{storage_input}.dfs.core.windows.net",
        input_storage_key
    )