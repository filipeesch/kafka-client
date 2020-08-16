namespace KafkaClient.Messages
{
    using System.IO;

    public class FetchV11Response : IResponse
    {
        public int ThrottleTimeMs { get; set; }

        public short ErrorCode { get; set; }

        public int SessionID { get; set; }

        public Topic[] Topics { get; set; }

        public void Read(Stream source)
        {
            this.ThrottleTimeMs = source.ReadInt32();
            this.ErrorCode = source.ReadInt16();
            this.SessionID = source.ReadInt32();
            this.Topics = source.ReadArray<Topic>();
        }

        public class Topic : IResponse
        {
            public string Name { get; set; }

            public Partition[] Partitions { get; set; }

            public void Read(Stream source)
            {
                this.Name = source.ReadString();
                this.Partitions = source.ReadArray<Partition>();
            }
        }

        public class Partition : IResponse
        {
            public PartitionHeader Header { get; set; }

            public RecordBatch RecordBatch { get; set; }

            public void Read(Stream source)
            {
                this.Header = source.ReadMessage<PartitionHeader>();
                this.RecordBatch = source.ReadMessage<RecordBatch>();
            }
        }

        public class PartitionHeader : IResponse
        {
            public int ID { get; set; }

            public short ErrorCode { get; set; }

            public long HighWatermark { get; set; }

            public long LastStableOffset { get; set; }

            public long LogStartOffset { get; set; }

            public AbortedTransaction[] AbortedTransactions { get; set; }

            public int PreferredReadReplica { get; set; }

            public void Read(Stream source)
            {
                this.ID = source.ReadInt32();
                this.ErrorCode = source.ReadInt16();
                this.HighWatermark = source.ReadInt64();
                this.LastStableOffset = source.ReadInt64();
                this.LogStartOffset = source.ReadInt64();
                this.AbortedTransactions = source.ReadArray<AbortedTransaction>();
                this.PreferredReadReplica = source.ReadInt32();
            }
        }

        public class AbortedTransaction : IResponse
        {
            public long ProducerID { get; set; }

            public long FirstOffset { get; set; }

            public void Read(Stream source)
            {
                this.ProducerID = source.ReadInt64();
                this.FirstOffset = source.ReadInt64();
            }
        }
    }
}
