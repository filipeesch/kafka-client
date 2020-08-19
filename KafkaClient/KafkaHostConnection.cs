namespace KafkaClient
{
    using System;
    using System.Buffers;
    using System.Buffers.Binary;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Net.Sockets;
    using System.Runtime.CompilerServices;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;

    public class KafkaHostConnection : IDisposable
    {
        private readonly TcpClient client;
        private readonly NetworkStream stream;

        private readonly ConcurrentDictionary<int, PendingRequest> pendingRequests = new ConcurrentDictionary<int, PendingRequest>();

        private int lastCorrelationId;

        private readonly CancellationTokenSource stopTokenSource = new CancellationTokenSource();
        private readonly Task listenerTask;

        private readonly byte[] messageSizeBuffer = new byte[sizeof(int)];

        private readonly string clientId;

        public KafkaHostConnection(string host, int port, string clientId)
        {
            this.clientId = clientId;
            this.client = new TcpClient(host, port);
            this.stream = this.client.GetStream();

            this.listenerTask = Task.Run(this.ListenStream);
        }

        private async Task ListenStream()
        {
            while (!this.stopTokenSource.IsCancellationRequested)
            {
                try
                {
                    var messageSize = await this.WaitForMessageSizeAsync().ConfigureAwait(false);

                    if (messageSize <= 0)
                        continue;

                    this.RespondMessage(new TrackedStream(this.stream), messageSize);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RespondMessage(Stream messageStream, int messageSize)
        {
            var correlationId = messageStream.ReadInt32();

            if (!this.pendingRequests.TryRemove(correlationId, out var request))
            {
                DiscardMessage(messageStream, messageSize);
                return;
            }

            var message = (IResponse) Activator.CreateInstance(request.ResponseType)!;

            if (message is IResponseV2)
                _ = messageStream.ReadTaggedFields();

            message.Read(messageStream);

            if (messageSize != messageStream.Position)
                throw new Exception("Some data was not read from response");

            request.CompletionSource.TrySetResult(message);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void DiscardMessage(Stream messageStream, in int messageSize)
        {
            messageStream.SkipBytes(messageSize - (int) messageStream.Position);
        }

        private async Task<int> WaitForMessageSizeAsync()
        {
            await this.stream
                .ReadAsync(this.messageSizeBuffer, 0, sizeof(int), this.stopTokenSource.Token)
                .ConfigureAwait(false);

            return BinaryPrimitives.ReadInt32BigEndian(this.messageSizeBuffer);
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
            this.stopTokenSource.Cancel();
            this.listenerTask.GetAwaiter().GetResult();
            this.listenerTask.Dispose();
            this.stream.Dispose();
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
