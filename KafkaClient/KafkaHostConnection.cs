namespace KafkaClient
{
    using System;
    using System.Collections.Concurrent;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;

    public class KafkaHostConnection : IDisposable
    {
        private readonly TcpClient client;
        private readonly NetworkStream stream;

        private readonly ConcurrentDictionary<int, PendindRequest> pendindRequests = new ConcurrentDictionary<int, PendindRequest>();

        private int correlationID;

        private readonly Task listenerTask;

        private readonly CancellationTokenSource stopTokenSource = new CancellationTokenSource();
        private readonly string cliendID;

        public KafkaHostConnection(string host, int port, string cliendID)
        {
            this.cliendID = cliendID;
            this.client = new TcpClient(host, port);
            this.stream = this.client.GetStream();

            this.listenerTask = Task.Run(this.ListenStream);
        }

        private async Task ListenStream()
        {
            while (!this.stopTokenSource.IsCancellationRequested)
            {
                while (!this.stream.DataAvailable)
                {
                    if (this.stopTokenSource.IsCancellationRequested)
                        return;

                    await Task.Delay(100).ConfigureAwait(false);
                }

                var response = new Response(this.stream);

                if (this.pendindRequests.TryRemove(response.CorrelationID, out var request))
                {
                    request.CompletionSource.TrySetResult(response.CreateMessage(request.ResponseType));
                }
            }
        }

        public Task<TResponse> SendAsync<TResponse>(IRequestMessage<TResponse> request, TimeSpan timeout)
            where TResponse : IResponse, new()
        {
            var pendindRequest = new PendindRequest(timeout, typeof(TResponse));

            var correlationId = this.NewCorrelationID();
            this.pendindRequests.TryAdd(correlationId, pendindRequest);

            this.stream.WriteMessage(
                new Request(
                    correlationId,
                    this.cliendID,
                    request));

            return pendindRequest.GetTask<TResponse>();
        }

        private int NewCorrelationID() => Interlocked.Increment(ref this.correlationID);

        public void Dispose()
        {
            this.stopTokenSource.Cancel();

            this.listenerTask.GetAwaiter().GetResult();

            this.client.Dispose();
        }

        private class PendindRequest
        {
            public TimeSpan Timeout { get; }

            public Type ResponseType { get; }

            public readonly TaskCompletionSource<IResponse> CompletionSource =
                new TaskCompletionSource<IResponse>();

            public PendindRequest(TimeSpan timeout, Type responseType)
            {
                this.Timeout = timeout;
                this.ResponseType = responseType;
            }

            public Task<TResponse> GetTask<TResponse>() where TResponse : IResponse =>
                this.CompletionSource.Task.ContinueWith(x => (TResponse) x.Result);
        }
    }
}
