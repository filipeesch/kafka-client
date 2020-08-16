namespace KafkaClient
{
    using System.IO;

    public class TopicMetadataResponse : IResponse
    {
        public void Read(Stream source)
        {
            this.Brokers = source.ReadMany<BrokerResponse>();
            this.ControllerID = source.ReadInt32();
            this.TopicsMetadata = source.ReadMany<TopicMetadata>();
        }

        public TopicMetadata[] TopicsMetadata { get; set; }

        public int ControllerID { get; private set; }

        public BrokerResponse[] Brokers { get; private set; }
    }
}