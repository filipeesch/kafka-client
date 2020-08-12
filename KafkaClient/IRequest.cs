namespace KafkaClient
{
    using System.IO;

    public interface IRequest
    {
        void Write(Stream destination);
    }
}
