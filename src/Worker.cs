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
    private readonly IEnumerable<IQuery> queries;

    // constructer injection by dependency injection
    public Worker(ILogger<Worker> logger,
        IPostgreService postgreService,
        IMongoService mongoService,
        IEnumerable<IQuery> queries)
    {
        _logger = logger;
        this._postgreService = postgreService;
        this._mongoService = mongoService;
        this.queries = queries;
    }
   
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Start migration.");

        // get data for all tables specified below
        foreach (var table in tables)
        {
            // data for all table + description
            var tableDescription = await _postgreService.LoadTableAsync(table);
            // add the dara to the mongo DB
            await _mongoService.AddTableAsync(tableDescription);
        }

        _logger.LogInformation("Migration finished");
        _logger.LogInformation("Start Queries");

        // Do queries
        foreach (var query in queries)
        {
            try
            {
                // execute the specified query for the exersice
                await query.RunAsync();
                // log the result if selects and the used query
                query.Log();
            }
            catch (Exception ex)
            {

            }
        }

        _logger.LogInformation("Queries finished");
    }
}

