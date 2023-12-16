using PostgreToMongo.Models;

namespace PostgreToMongo.Services
{
    public interface IPostgreService
    {
        Task<TableDescription> LoadTableAsync(string table);
    }
}