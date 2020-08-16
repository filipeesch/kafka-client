namespace KafkaClient.Messages
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    public class TopicMetadataV1Request : IRequestMessage<TopicMetadataV1Response>
    {
        public ApiKey ApiKey => ApiKey.Metadata;

        public short ApiVersion => 1;

        public string[] Topics { get; }

        public TopicMetadataV1Request(IEnumerable<string> topics)
        {
            this.Topics = topics.ToArray();
        }

        public void Write(Stream destination)
        {
            destination.WriteInt32(this.Topics.Length);

            foreach (var topic in this.Topics)
            {
                destination.WriteString(topic);
            }
        }
    }
}
