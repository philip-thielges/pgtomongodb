using System;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries;

public class Aufgabe5a : Query
{
    private static readonly string call = @"
        var collection = Database.GetCollection<BsonDocument>(""staff"");
        using var sha1 = SHA1.Create();

        var hash = Convert.ToHexString(sha1.ComputeHash(Encoding.UTF8.GetBytes(""mbnXXstYD#&S6dS6xeBZ3"")));
        var hash2 = Convert.ToHexString(sha1.ComputeHash(Encoding.UTF8.GetBytes(""7LAN3M57527kZ8^GM#Cc^"")));
        /* 
         * Da in der original DB auch SHA1 genutzt wurde nutzen wir nun auch wieder SHA1,
         * um mit dem ursprünglichen System kompatibel zu bleiben.
         * Das Passwort wurde von BItWarden generiert und sollte auch dort gespeichert werden, Passwörter sollte man sich nicht merken können.
         * Wie man es eigentlich machen sollte:
         * - Nutzen einer Key derivation Function wie: PBKDF2 oder bcrypt
         * - PBKDF2 nutzt intern zwar auch einen SHA-X HMAC (man sollte SHA2 wegen der Collisions resistence nutzen), aber man kann hier die Anzahl an Iterationen so wie ein Salt mitgeben.
         * - Ein Salt verhindert, dass zwei mal das selbe Passwort auch den selben Hash ergeben.
         * - Die Iterationen verlangsamen den Prozess
         */
        collection.UpdateOne(""{staff_id : 1}"", ""{$set: { \""password\"" : \"""" + hash + ""\""}}"");
        collection.UpdateOne(""{staff_id : 2}"", ""{$set: { \""password\"" : \"""" + hash2 + ""\""}}"");";

    public Aufgabe5a(ILogger<Query> logger,
        IOptions<MongoSettings> mongoSettings)
        : base(call, "Aufgabe 5a", logger, mongoSettings)
    {
    }

    public async override Task RunAsync()
    {
        var collection = Database.GetCollection<BsonDocument>("staff");
        using var sha1 = SHA1.Create();

        var hash = Convert.ToHexString(sha1.ComputeHash(Encoding.UTF8.GetBytes("mbnXXstYD#&S6dS6xeBZ3")));
        var hash2 = Convert.ToHexString(sha1.ComputeHash(Encoding.UTF8.GetBytes("7LAN3M57527kZ8^GM#Cc^")));
        /* 
         * Da in der original DB auch SHA1 genutzt wurde nutzen wir nun auch wieder SHA1,
         * um mit dem ursprünglichen System kompatibel zu bleiben.
         * Das Passwort wurde von BItWarden generiert und sollte auch dort gespeichert werden, Passwörter sollte man sich nicht merken können.
         * Wie man es eigentlich machen sollte:
         * - Nutzen einer Key derivation Function wie: PBKDF2 oder bcrypt
         * - PBKDF2 nutzt intern zwar auch einen SHA-X HMAC (man sollte SHA2 wegen der Collisions resistence nutzen), aber man kann hier die Anzahl an Iterationen so wie ein Salt mitgeben.
         * - Ein Salt verhindert, dass zwei mal das selbe Passwort auch den selben Hash ergeben.
         * - Die Iterationen verlangsamen den Prozess
         */
        collection.UpdateOne("{staff_id : 1}", "{$set: { \"password\" : \"" + hash + "\"}}");
        collection.UpdateOne("{staff_id : 2}", "{$set: { \"password\" : \"" + hash2 + "\"}}");

    }
}

