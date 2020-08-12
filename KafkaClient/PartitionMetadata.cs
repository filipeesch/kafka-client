namespace KafkaClient
{
    using System.IO;

    public class PartitionMetadata : IResponseMessage
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
}
