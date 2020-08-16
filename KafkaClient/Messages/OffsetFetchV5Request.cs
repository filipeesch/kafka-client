namespace KafkaClient.Messages
{
    using System;
    using System.IO;

    public class OffsetFetchV5Request : IRequestMessage<OffsetFetchV5Response>
    {
        public OffsetFetchV5Request(string groupId, Topic[] topics)
        {
            this.GroupID = groupId;
            this.Topics = topics;
        }

        public ApiKey ApiKey => ApiKey.OffsetFetch;

        public short ApiVersion => 5;

        public string GroupID { get; }

        public Topic[] Topics { get; }

        public void Write(Stream destination)
        {
            destination.WriteString(this.GroupID);
            destination.WriteArray(this.Topics);
        }

        public class Topic : IRequest
        {
            public Topic(string name, int[] partitions)
            {
                this.Name = name;
                this.Partitions = partitions;
            }

            public string Name { get; }

            public int[] Partitions { get; }

            public void Write(Stream destination)
            {
                destination.WriteString(this.Name);
                destination.WriteInt32Array(this.Partitions);
            }
        }
    }
}
