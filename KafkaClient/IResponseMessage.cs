namespace KafkaClient
{
    using System.IO;

    public interface IResponseMessage
    {
        void Read(Stream source);
    }
}
