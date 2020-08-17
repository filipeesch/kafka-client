namespace KafkaClient.Messages
{
    using System.IO;

    public class FindCoordinatorV3Response : IResponseV2
    {
        public int ThrottleTimeMs { get; private set; }

        public short ErrorCode { get; private set; }

        public string ErrorMessage { get; private set; }

        public int NodeID { get; private set; }

        public string Host { get; private set; }

        public int Port { get; private set; }

        public TaggedField[] TaggedFields { get; private set; }

        public void Read(Stream source)
        {
            this.ThrottleTimeMs = source.ReadInt32();
            this.ErrorCode = source.ReadInt16();
            this.ErrorMessage = source.ReadCompactString();
            this.NodeID = source.ReadInt32();
            this.Host = source.ReadCompactString();
            this.Port = source.ReadInt32();
            this.TaggedFields = source.ReadTaggedFields();
        }
    }
}