namespace KafkaClient
{
    using System.IO;

    public class TopicMetadata : IResponseMessage
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
}
