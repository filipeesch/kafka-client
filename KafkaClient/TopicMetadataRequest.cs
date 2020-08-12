namespace KafkaClient
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    public class TopicMetadataRequest : IRequestMessage<TopicMetadataResponse>
    {
        public string[] Topics { get; }

        public TopicMetadataRequest(IEnumerable<string> topics)
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

        public ApiKey ApiKey => ApiKey.Metadata;

        public short ApiVersion => 1;
    }
}
