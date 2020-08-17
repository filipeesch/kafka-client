namespace KafkaClient.Messages
{
    using System.IO;

    public class TopicMetadataV9Response : IResponseV2
    {
        public int ThrottleTime { get; private set; }
        public Broker[] Brokers { get; private set; }
        public string ClusterID { get; private set; }
        public int ControllerID { get; private set; }
        public Topic[] Topics { get; private set; }
        public int ClusterAuthorizedOperations { get; private set; }
        public TaggedField[] TaggedFields { get; private set; }

        public void Read(Stream source)
        {
            this.ThrottleTime = source.ReadInt32();
            this.Brokers = source.ReadCompactArray<Broker>();
            this.ClusterID = source.ReadCompactString();
            this.ControllerID = source.ReadInt32();
            this.Topics = source.ReadCompactArray<Topic>();
            this.ClusterAuthorizedOperations = source.ReadInt32();
            this.TaggedFields = source.ReadTaggedFields();
        }

        public class Topic : IResponseV2
        {
            public short ErrorCode { get; private set; }
            public string Name { get; private set; }
            public bool IsInternal { get; private set; }
            public Partition[] Partitions { get; private set; }
            public int TopicAuthorizedOperations { get; set; }
            public TaggedField[] TaggedFields { get; private set; }

            public void Read(Stream source)
            {
                this.ErrorCode = source.ReadInt16();
                this.Name = source.ReadCompactString();
                this.IsInternal = source.ReadBoolean();
                this.Partitions = source.ReadCompactArray<Partition>();
                this.TopicAuthorizedOperations = source.ReadInt32();
                this.TaggedFields = source.ReadTaggedFields();
            }
        }

        public class Partition : IResponseV2
        {
            public short ErrorCode { get; set; }
            public int ID { get; private set; }
            public int LeaderID { get; private set; }
            public int LeaderEpoch { get; private set; }
            public int[] ReplicaNodes { get; private set; }
            public int[] IsrNodes { get; private set; }
            public int[] OfflineReplicas { get; private set; }
            public TaggedField[] TaggedFields { get; private set; }

            public void Read(Stream source)
            {
                this.ErrorCode = source.ReadInt16();
                this.ID = source.ReadInt32();
                this.LeaderID = source.ReadInt32();
                this.LeaderEpoch = source.ReadInt32();
                this.ReplicaNodes = source.ReadCompactInt32Array();
                this.IsrNodes = source.ReadCompactInt32Array();
                this.OfflineReplicas = source.ReadCompactInt32Array();
                this.TaggedFields = source.ReadTaggedFields();
            }
        }

        public class Broker : IResponseV2
        {
            public int NodeId { get; private set; }
            public string Host { get; private set; }
            public int Port { get; private set; }
            public string Rack { get; private set; }
            public TaggedField[] TaggedFields { get; private set; }

            public void Read(Stream source)
            {
                this.NodeId = source.ReadInt32();
                this.Host = source.ReadCompactString();
                this.Port = source.ReadInt32();
                this.Rack = source.ReadCompactString();
                this.TaggedFields = source.ReadTaggedFields();
            }
        }
    }
}
