namespace PostgreToMongo.Queries
{
    public interface IQuery
    {
        void Log();
        Task RunAsync();
    }
}