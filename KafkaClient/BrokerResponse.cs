namespace KafkaClient
{
    using System.IO;

    public class BrokerResponse : IResponseMessage
    {
        public int NodeId { get; private set; }

        public string Host { get; private set; }

        public int Port { get; private set; }

        public string Rack { get; private set; }

        public void Read(Stream source)
        {
            this.NodeId = source.ReadInt32();
            this.Host = source.ReadString();
            this.Port = source.ReadInt32();
            this.Rack = source.ReadString();
        }
    }
}