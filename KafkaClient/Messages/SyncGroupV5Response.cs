namespace KafkaClient.Messages
{
    using System.IO;

    public class SyncGroupV5Response : IResponseV2
    {
        public int ThrottleTimeMs { get; private set; }

        public ErrorCode Error { get; private set; }

        public string? ProtocolType { get; private set; }

        public string? ProtocolName { get; private set; }

        public byte[] AssignmentMetadata { get; private set; }

        public TaggedField[] TaggedFields { get; private set; }

        public void Read(Stream source)
        {
            this.ThrottleTimeMs = source.ReadInt32();
            this.Error = source.ReadErrorCode();
            this.ProtocolType = source.ReadCompactNullableString();
            this.ProtocolName = source.ReadCompactNullableString();
            this.AssignmentMetadata = source.ReadCompactByteArray();
            this.TaggedFields = source.ReadTaggedFields();
        }
    }
}
