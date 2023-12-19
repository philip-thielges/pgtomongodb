using Microsoft.Extensions.Configuration;
using PostgreToMongo;
using PostgreToMongo.Options;
using PostgreToMongo.Queries;
using PostgreToMongo.Services;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        IConfiguration config = context.Configuration;

        // Add options
        services.Configure<MongoSettings>(config.GetSection(nameof(MongoSettings)));
        services.Configure<PostgreSettings>(config.GetSection(nameof(PostgreSettings)));

        // Mongo
        // Add mongo db services to the container.
        services.AddScoped<IMongoService, MongoService>();
        // PostgreSQL service
        services.AddScoped<IPostgreService, PostgreService>();

        // Add Queries
        services.AddScoped<IQueryBuilder, Aufgabe4a>();
        services.AddScoped<IQueryBuilder, Aufgabe4b>();
        services.AddScoped<IQueryBuilder, Aufgabe4c>();
        services.AddScoped<IQueryBuilder, Aufgabe4d>();
        services.AddScoped<IQueryBuilder, Aufgabe4e>();
        services.AddScoped<IQueryBuilder, Aufgabe4f>();
        services.AddScoped<IQueryBuilder, Aufgabe4g>();
        services.AddScoped<IQueryBuilder, Aufgabe4h>();
        services.AddScoped<IQueryBuilder, Aufgabe4i>();

        services.AddScoped<IQueryBuilder, Aufgabe5a>();
        services.AddScoped<IQueryBuilder, Aufgabe5b>();

        services.AddScoped<IQueryBuilder, Aufgabe6>();

        services.AddHostedService<Worker>();
    })
    .Build();

host.Run();

