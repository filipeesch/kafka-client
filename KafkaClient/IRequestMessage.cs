namespace KafkaClient
{
    public interface IRequestMessage<TResponse> : IRequestMessage where TResponse : IResponseMessage
    {
    }

    public interface IRequestMessage : IRequest
    {
        ApiKey ApiKey { get; }

        short ApiVersion { get; }
    }
}
