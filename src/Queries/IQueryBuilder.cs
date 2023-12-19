namespace PostgreToMongo.Queries
{
    public interface IQueryBuilder
    {
        void PerformLogging();
        Task ExecuteAsync();
    }
}