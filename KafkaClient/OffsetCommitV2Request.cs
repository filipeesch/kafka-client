namespace KafkaClient
{
    using System.IO;

    public class OffsetCommitV2Request : IRequestMessage<OffsetCommitV2Response>
    {
        public OffsetCommitV2Request(
            string groupId,
            int groupGenerationId,
            string memberId,
            long retentionTimeMs,
            Topic[] topics)
        {
            this.GroupID = groupId;
            this.GroupGenerationID = groupGenerationId;
            this.MemberID = memberId;
            this.RetentionTimeMs = retentionTimeMs;
            this.Topics = topics;
        }

        public string GroupID { get; }

        public int GroupGenerationID { get; }

        public string MemberID { get; }

        public long RetentionTimeMs { get; }

        public Topic[] Topics { get; }

        public ApiKey ApiKey => ApiKey.OffsetCommit;

        public short ApiVersion => 2;

        public void Write(Stream destination)
        {
            destination.WriteString(this.GroupID);
            destination.WriteInt32(this.GroupGenerationID);
            destination.WriteInt64(this.RetentionTimeMs);
            destination.WriteMany(this.Topics);
        }

        public class Topic : IRequest
        {
            public Topic(string name, Partition[] partitions)
            {
                this.Name = name;
                this.Partitions = partitions;
            }

            public string Name { get; }

            public Partition[] Partitions { get; }

            public void Write(Stream destination)
            {
                destination.WriteString(this.Name);
                destination.WriteMany(this.Partitions);
            }
        }

        public class Partition : IRequest
        {
            public int ID { get; }

            public long Offset { get; }

            public string Metadata { get; }

            public void Write(Stream destination)
            {
                destination.WriteInt32(this.ID);
                destination.WriteInt64(this.Offset);
                destination.WriteString(this.Metadata);
            }
        }
    }
}
