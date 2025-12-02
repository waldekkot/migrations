# run on local (MacOS) Docker

# SQL Server 2017
docker run -d \
  --platform linux/amd64 \
  --name sqlserver2017 \
  -e 'ACCEPT_EULA=Y' \
  -e "MSSQL_SA_PASSWORD=$ADMIN_PASS" \
  -e 'MSSQL_PID=Developer' \
  -p 1433:1433 \
  -v "$(pwd)/AdventureWorks:/var/opt/mssql/backup" \
  -v sqlserver-data-2017:/var/opt/mssql \
  mcr.microsoft.com/mssql/server:2017-latest

# you can also use other SQL Server images
# mcr.microsoft.com/mssql/server:2022-latest
# mcr.microsoft.com/mssql/server:2019-latest
# mcr.microsoft.com/mssql/server:2017-latest

# connect
sqlcmd -S 127.0.0.1,1433 -U sa -P "$ADMIN_PASS" -C -Q "SELECT name FROM sys.databases"

# cleanup


