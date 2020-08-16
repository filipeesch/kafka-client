namespace KafkaClient.Messages
{
    using System.IO;

    public class TopicMetadataV1Response : IResponse
    {
        public void Read(Stream source)
        {
            this.Brokers = source.ReadMany<BrokerResponse>();
            this.ControllerID = source.ReadInt32();
            this.TopicsMetadata = source.ReadMany<TopicMetadata>();
        }

        public TopicMetadata[] TopicsMetadata { get; set; }

        public int ControllerID { get; private set; }

        public BrokerResponse[] Brokers { get; private set; }

        public class TopicMetadata : IResponse
        {
            public short ErrorCode { get; private set; }

            public string Topic { get; private set; }

            public bool IsInternal { get; private set; }

            public PartitionMetadata[] PartitionsMetadata { get; private set; }

            public void Read(Stream source)
            {
                this.ErrorCode = source.ReadInt16();
                this.Topic = source.ReadString();
                this.IsInternal = source.ReadBoolean();
                this.PartitionsMetadata = source.ReadMany<PartitionMetadata>();
            }
        }

        public class PartitionMetadata : IResponse
        {
            public short ErrorCode { get; set; }

            public int ID { get; private set; }

            public int Leader { get; private set; }

            public int[] Replicas { get; private set; }

            public int[] Isrs { get; private set; }

            public void Read(Stream source)
            {
                this.ErrorCode = source.ReadInt16();
                this.ID = source.ReadInt32();
                this.Leader = source.ReadInt32();
                this.Replicas = source.ReadManyInt32();
                this.Isrs = source.ReadManyInt32();
            }
        }

        public class BrokerResponse : IResponse
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
}
