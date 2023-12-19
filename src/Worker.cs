using System.ComponentModel.DataAnnotations.Schema;
using PostgreToMongo.Models;
using PostgreToMongo.Queries;
using PostgreToMongo.Services;

namespace PostgreToMongo;

public class Worker : BackgroundService
{
    // A list of all tables with data in the postgre DB
    private static readonly string[] tables = new string[] { "actor", "address", "category", "city", "country", "customer", "film", "film_actor", "film_category", "inventory", "language", "payment", "rental", "staff", "store" };

    private readonly ILogger<Worker> _logger;
    private readonly IPostgreService _postgreService;
    private readonly IMongoService _mongoService;
    private readonly IEnumerable<IQueryBuilder> queries;

    // constructer injection by dependency injection
    public Worker(ILogger<Worker> logger,
        IPostgreService postgreService,
        IMongoService mongoService,
        IEnumerable<IQueryBuilder> queries)
    {
        _logger = logger;
        this._postgreService = postgreService;
        this._mongoService = mongoService;
        this.queries = queries;
    }
   
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Start migration:\n------------------------------------\n");

        // get data for all tables specified below
        foreach (var table in tables)
        {
            // data for all table + description
            var tableDescription = await _postgreService.LoadTableAsync(table);
            // add the dara to the mongo DB
            await _mongoService.AddTableAsync(tableDescription);
        }

        _logger.LogInformation("\n------------------------------------\nMigration finished\n-----------------------------------");
        _logger.LogInformation("\n-------------------------------------\nStart Queries\n------------------------------------\n");

        // Do queries
        foreach (var query in queries)
        {
            try
            {
                // execute the specified query for the exersice
                await query.ExecuteAsync();
                // log the result if selects and the used query
                query.PerformLogging();
            }
            catch (Exception ex)
            {

            }
        }

        _logger.LogInformation("\n------------------------------------\nQueries finished");
    }
}

