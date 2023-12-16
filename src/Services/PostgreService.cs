using System;
using System.Data;
using System.Data.Common;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using PostgreToMongo.Models;
using PostgreToMongo.Options;

namespace PostgreToMongo.Services
{
    public class PostgreService : IAsyncDisposable, IPostgreService
    {
        private readonly NpgsqlDataSource _npgsqlDataSource;
        private readonly ILogger<PostgreService> _logger;

        // get related service and options via DI
        public PostgreService(ILogger<PostgreService> logger,
            IOptions<PostgreSettings> postgreSettings)
        {
            _npgsqlDataSource = NpgsqlDataSource.Create(postgreSettings.Value.ConnectionString);
            _logger = logger;
        }

        // close the DB connection when the service will be disposed
        public ValueTask DisposeAsync()
        {
            if (_npgsqlDataSource is not null)
                return _npgsqlDataSource.DisposeAsync();
            return default;
        }

        /// <summary>
        /// Load a table description, which contains also the data, from the database.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public async Task<TableDescription> LoadTableAsync(string table)
        {
            _logger.LogInformation($"Start loading table '{table}'.");
            var tableDescription = new TableDescription { TableName = table };

            // Retrieve all rows in the given table
            // await using, will asynchronously dispose the command object if it is no longer needed.
            await using (var cmd = _npgsqlDataSource.CreateCommand($"SELECT * FROM \"{table}\""))
            // execute the command and retrieve the data
            // also get all key infos for the data table schema, this is needed to get infos about the column, etc. (datatypes...)
            await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.KeyInfo))
            {
                // get the schmea for the table
                var schema = await reader.GetColumnSchemaAsync();

                // schema is a list, so we can form a List of columndescriptions out of it.
                tableDescription.ColumnDescriptions = schema.Select(x => new ColumnDescription
                {
                    Name = x.ColumnName,
                    // IsKey is a nullable boolean so it wrapped, thats why we did an optional null check here. if this is null we assume it is not a PK.
                    IsPrimaryKey = x.IsKey??false,
                    DataTypeName = x.DataTypeName
                }).ToList();

                // read all rows.
                while (await reader.ReadAsync())
                {
                    // create a new dictionary, which will represent one row of data.
                    // key is the column name, val is the data in this row and column.
                    var row = new Dictionary<string, object>();

                    // for all columns...
                    foreach (var item in tableDescription.ColumnDescriptions)
                    {
                        // read the data
                        var val = reader[item.Name];
                        // set the data by column name
                        // switch for some specific speacial cases:
                        row[item.Name] = val switch
                        {
                            // DBNull, wont be understanded, thats why we just return a common null
                            // !<-- because we know its null and this null is also wanted.
                            DBNull => null!,
                            // Vectors are not easily supported in mongo. Thats why we convert it to an dictionary.
                            NpgsqlTsVector vector => vector.ToDictionary(k => k.Text, v => v[0].Pos),
                            // in all other cases just write the value as data into the row.
                            _ => val
                        } ;
                    }

                    // ad the row to the table.
                    tableDescription.Table.Add(row);
                }
            }

            return tableDescription;
        }
    }
}

