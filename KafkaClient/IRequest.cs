namespace KafkaClient
{
    using System.IO;
    using KafkaClient.Messages;

    public interface IRequest
    {
        void Write(Stream destination);
    }

    public interface IRequestV2 : IRequest
    {
        TaggedField[] TaggedFields { get; }
    }
}
