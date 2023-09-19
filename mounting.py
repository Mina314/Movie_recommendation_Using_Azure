adlsAccountName= 'mllandingzone'
adlsContainerName = 'validated'
adlsFolderName='Data'
mountPoint='/mnt/Files/Validated'

# Application (client) ID
applicationId=dbutils.secrets.get(scope='test',key='clieid')

# Application (client) Secret Key
authenticationKey=dbutils.secrets.get(scope='test',key='clisecret')

# Directory (tenant) ID
tenandId=dbutils.secrets.get(scope='test',key='tenid')

endpoint='https://login.microsoftonline.com/'+tenandId+'/oauth2/token'
source='abfss://'+adlsContainerName+'@'+adlsAccountName+'.dfs.core.windows.net/'+adlsFolderName

configs={"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": applicationId,
        "fs.azure.account.oauth2.client.secret": authenticationKey,
        "fs.azure.account.oauth2.client.endpoint": endpoint}

# Mounting ADLS Storage to DBFS
# Mount only if the directory is not already mounted
if not any(mount.mountPoint==mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=source,
        mount_point=mountPoint,
        extra_configs=configs)