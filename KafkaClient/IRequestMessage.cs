namespace KafkaClient
{
    public interface IRequestMessage<TResponse> : IRequestMessage where TResponse : IResponse
    {
    }

    public interface IRequestMessage : IRequest
    {
        ApiKey ApiKey { get; }

        short ApiVersion { get; }
    }
}
