namespace KafkaClient
{
    using System.IO;

    public class OffsetCommitV2Response : IResponse
    {
        public Topic[] Topics { get; private set; }

        public void Read(Stream source)
        {
            this.Topics = source.ReadMany<Topic>();
        }

        public class Topic : IResponse
        {
            public string Name { get; private set; }

            public Partition[] Partitions { get; private set; }

            public void Read(Stream source)
            {
                this.Name = source.ReadString();
                this.Partitions = source.ReadMany<Partition>();
            }
        }

        public class Partition : IResponse
        {
            public int ID { get; private set; }

            public short ErrorCode { get; private set; }

            public void Read(Stream source)
            {
                this.ID = source.ReadInt32();
                this.ErrorCode = source.ReadInt16();
            }
        }
    }
}
