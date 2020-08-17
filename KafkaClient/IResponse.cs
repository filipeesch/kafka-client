namespace KafkaClient
{
    using System.IO;
    using KafkaClient.Messages;

    public interface IResponse
    {
        void Read(Stream source);
    }

    public interface IResponseV2 : IResponse
    {
        TaggedField[] TaggedFields { get; }
    }
}
