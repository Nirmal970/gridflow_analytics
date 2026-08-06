from pyspark.dbutils import DBUtils


def configure_adls(spark):

    dbutils = DBUtils(spark)

    storage_account = "gridflowstoragedev"

    scope = "gridflow-dev-adls"

    tenant_id = dbutils.secrets.get(scope=scope,key="tenant-id")

    client_id = dbutils.secrets.get(scope=scope,key="client-id")

    client_secret = dbutils.secrets.get(scope=scope,key="client-secret")

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net","OAuth")

    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",client_id)

    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",client_secret)

    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

    print("ADLS authentication configured successfully.")