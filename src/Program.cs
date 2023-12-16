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
        // Aufgabe 4...
        services.AddScoped<IQuery, Aufgabe4a>();
        services.AddScoped<IQuery, Aufgabe4b>();
        services.AddScoped<IQuery, Aufgabe4c>();
        services.AddScoped<IQuery, Aufgabe4d>();
        services.AddScoped<IQuery, Aufgabe4e>();
        services.AddScoped<IQuery, Aufgabe4f>();
        services.AddScoped<IQuery, Aufgabe4g>();
        services.AddScoped<IQuery, Aufgabe4h>();
        services.AddScoped<IQuery, Aufgabe4i>();

        // Aufgabe 5
        services.AddScoped<IQuery, Aufgabe5a>();
        services.AddScoped<IQuery, Aufgabe5b>();

        // Aufgabe 6...
        services.AddScoped<IQuery, Aufgabe6>();

        services.AddHostedService<Worker>();
    })
    .Build();

host.Run();

