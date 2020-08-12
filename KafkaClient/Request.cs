namespace KafkaClient
{
    using System;
    using System.IO;

    public class Request : IRequest
    {
        public int CorrelationId { get; }
        public string ClientId { get; }
        public IRequestMessage Message { get; }

        public Request(
            int correlationId,
            string clientId,
            IRequestMessage message)
        {
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
            this.Message = message;
        }

        public void Write(Stream destination)
        {
            using var tmp = new MemoryStream();

            tmp.WriteInt16((short) this.Message.ApiKey);
            tmp.WriteInt16(this.Message.ApiVersion);
            tmp.WriteInt32(this.CorrelationId);
            tmp.WriteString(this.ClientId);
            tmp.WriteMessage(this.Message);

            destination.WriteInt32(Convert.ToInt32(tmp.Length));

            tmp.Position = 0;
            tmp.CopyTo(destination);
        }
    }
}