using PostgreToMongo.Models;

namespace PostgreToMongo.Services
{
    public interface IMongoService
    {
        Task AddTableAsync(TableDescription tableDescription);
    }
}