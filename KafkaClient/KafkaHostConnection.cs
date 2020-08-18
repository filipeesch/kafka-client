namespace KafkaClient
{
    using System;
    using System.Buffers;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;

    public class KafkaHostConnection : IDisposable
    {
        private readonly TcpClient client;
        private readonly NetworkStream stream;

        private readonly ConcurrentDictionary<int, PendingRequest> pendingRequests = new ConcurrentDictionary<int, PendingRequest>();

        private int lastCorrelationId;

        private readonly Timer listenerTimer;

        private readonly string clientId;

        public KafkaHostConnection(string host, int port, string clientId)
        {
            this.clientId = clientId;
            this.client = new TcpClient(host, port);
            this.stream = this.client.GetStream();

            this.listenerTimer = new Timer(
                _ => this.ListenStream(),
                null,
                0,
                100);
        }

        private void ListenStream()
        {
            if (!this.stream.DataAvailable)
                return;

            var messageSize = this.stream.ReadInt32();

            var tmp = ArrayPool<byte>.Shared.Rent(messageSize);

            try
            {
                this.stream.Read(tmp, 0, messageSize);

                using var messageStream = new MemoryStream(tmp, 0, messageSize);

                var correlationId = messageStream.ReadInt32();

                if (!this.pendingRequests.TryRemove(correlationId, out var request))
                {
                    return;
                }

                var message = (IResponse) Activator.CreateInstance(request.ResponseType)!;

                if (message is IResponseV2)
                    _ = messageStream.ReadTaggedFields();

                message.Read(messageStream);

                if (messageStream.Length != messageStream.Position)
                    throw new Exception("Some data was not read from response");

                request.CompletionSource.TrySetResult(message);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(tmp);
            }
        }

        public Task<TResponse> SendAsync<TResponse>(IRequestMessage<TResponse> request, TimeSpan timeout)
            where TResponse : IResponse, new()
        {
            var pendingRequest = new PendingRequest(
                timeout,
                typeof(TResponse));

            lock (this.stream)
            {
                this.pendingRequests.TryAdd(++this.lastCorrelationId, pendingRequest);

                this.stream.WriteMessage(
                    new Request(
                        this.lastCorrelationId,
                        this.clientId,
                        request));
            }

            return pendingRequest.GetTask<TResponse>();
        }

        public void Dispose()
        {
            this.listenerTimer.Dispose();
            this.client.Dispose();
        }

        private class PendingRequest
        {
            public TimeSpan Timeout { get; }

            public Type ResponseType { get; }

            public readonly TaskCompletionSource<IResponse> CompletionSource =
                new TaskCompletionSource<IResponse>();

            public PendingRequest(TimeSpan timeout, Type responseType)
            {
                this.Timeout = timeout;
                this.ResponseType = responseType;
            }

            public Task<TResponse> GetTask<TResponse>() where TResponse : IResponse =>
                this.CompletionSource.Task.ContinueWith(x => (TResponse) x.Result);
        }
    }
}
