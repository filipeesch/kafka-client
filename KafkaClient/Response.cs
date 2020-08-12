namespace KafkaClient
{
    using System;
    using System.IO;

    public readonly struct Response
    {
        private readonly byte[] payload;

        public Response(Stream source)
        {
            var messageSize = source.ReadInt32() - sizeof(int); // CorrelationID bytes
            this.CorrelationID = source.ReadInt32();

            this.payload = new byte[messageSize];
            source.Read(this.payload);
        }

        public int CorrelationID { get; }

        public IResponseMessage CreateMessage(Type responseType)
        {
            var message = (IResponseMessage) Activator.CreateInstance(responseType);

            using var ms = new MemoryStream(this.payload);
            message.Read(ms);

            return message;
        }
    }
}
