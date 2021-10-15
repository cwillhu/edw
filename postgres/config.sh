
#set wal_level
az postgres server configuration set \
    --resource-group dbmihds-poc-postgres-rg \
    --server-name dbmihds-pg-srv \
    --name azure.replication_support --value replica

az postgres server configuration show \
    --resource-group dbmihds-poc-postgres-rg \
    --server-name dbmihds-pg-srv \
    --name azure.replication_support

az postgres server restart --resource-group dbmihds-poc-postgres-rg --name dbmihds-pg-srv
