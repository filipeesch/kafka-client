namespace KafkaClient.Messages
{
    using System.IO;

    public class ProduceV8Response : IResponse
    {
        public Topic[] Topics { get; set; }

        public int ThrottleTimeMs { get; set; }

        public void Read(Stream source)
        {
            this.Topics = source.ReadMany<Topic>();
            this.ThrottleTimeMs = source.ReadInt32();
        }

        public class Topic : IResponse
        {
            public string Name { get; set; }

            public Partition[] Partitions { get; set; }

            public void Read(Stream source)
            {
                this.Name = source.ReadString();
                this.Partitions = source.ReadMany<Partition>();
            }
        }

        public class Partition : IResponse
        {
            public int ID { get; set; }

            public short ErrorCode { get; set; }

            public long Offset { get; set; }

            public long LogAppendTime { get; set; }

            public long LogStartOffset { get; set; }

            public RecordError[] Errors { get; set; }

            public void Read(Stream source)
            {
                this.ID = source.ReadInt32();
                this.ErrorCode = source.ReadInt16();
                this.Offset = source.ReadInt64();
                this.LogAppendTime = source.ReadInt64();
                this.LogStartOffset = source.ReadInt64();
                this.Errors = source.ReadMany<RecordError>();
            }
        }

        public class RecordError : IResponse
        {
            public int BatchIndex { get; set; }

            public string Message { get; set; }

            public void Read(Stream source)
            {
                this.BatchIndex = source.ReadInt32();
                this.Message = source.ReadString();
            }
        }
    }
}
