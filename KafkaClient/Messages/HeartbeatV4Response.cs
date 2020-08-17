namespace KafkaClient.Messages
{
    using System.IO;

    public class HeartbeatV4Response : IResponseV2
    {
        public int ThrottleTimeMs { get; private set; }

        public GroupError ErrorCode { get; private set; }

        public TaggedField[] TaggedFields { get; private set; }

        public void Read(Stream source)
        {
            this.ThrottleTimeMs = source.ReadInt32();
            this.ErrorCode = (GroupError) source.ReadInt16();
            this.TaggedFields = source.ReadTaggedFields();
        }
    }
}
