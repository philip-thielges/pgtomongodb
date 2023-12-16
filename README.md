# Übung Datenbanken

Start the docker compose with
> docker compose up -d --build
To see the logs run the docker compose command without `-d` (logs of all containers!)

## See logs

The logs will be printed to the console (stdout) of the respectively service.

## End of migration

The migration and querying is completed, if the stdout of the container shows `Queries finished`. This may need a few minutes!  
Now if needed the stdout logs of the container can be saved to a file with the following command: 
> docker logs migrationservice >& migrationservice.log

Now the `migrationservice.log` contains all the logs.

## Contributors

- Florian (50 %)
  - Migration
  - Coding
- Timo (50 %)
  - Queries
    - SQL + Mongo
