namespace KafkaClient
{
    using System.IO;

    public interface IResponse
    {
        void Read(Stream source);
    }
}
