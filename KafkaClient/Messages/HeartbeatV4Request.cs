namespace KafkaClient.Messages
{
    using System;
    using System.IO;

    public class HeartbeatV4Request : IRequestMessageV2<HeartbeatV4Response>
    {
        public HeartbeatV4Request(string groupId, int generationId, string memberId)
        {
            this.GroupID = groupId;
            this.GenerationID = generationId;
            this.MemberID = memberId;
        }

        public ApiKey ApiKey => ApiKey.Heartbeat;

        public short ApiVersion => 4;

        public string GroupID { get; }

        public int GenerationID { get; }

        public string MemberID { get; }

        public string GroupInstanceID { get; set; }

        public TaggedField[] TaggedFields => Array.Empty<TaggedField>();

        public void Write(Stream destination)
        {
            destination.WriteCompactString(this.GroupID);
            destination.WriteInt32(this.GenerationID);
            destination.WriteCompactString(this.MemberID);
            destination.WriteCompactString(this.GroupInstanceID);
            destination.WriteTaggedFields(this.TaggedFields);
        }
    }
}
