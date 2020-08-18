namespace KafkaClient.Messages
{
    using System.IO;

    public class LeaveGroupV4Response : IResponseV2
    {
        public int ThrottleTimeMs { get; private set; }
        public ErrorCode Error { get; private set; }
        public Member[] Members { get; private set; }

        public TaggedField[] TaggedFields { get; private set; }

        public void Read(Stream source)
        {
            this.ThrottleTimeMs = source.ReadInt32();
            this.Error = (ErrorCode) source.ReadInt16();
            this.Members = source.ReadCompactArray<Member>();
            this.TaggedFields = source.ReadTaggedFields();
        }

        public class Member : IResponseV2
        {
            public string MemberId { get; private set; }

            public string? GroupInstanceId { get; private set; }

            public ErrorCode Error { get; private set; }

            public TaggedField[] TaggedFields { get; private set; }

            public void Read(Stream source)
            {
                this.MemberId = source.ReadCompactString();
                this.GroupInstanceId = source.ReadCompactString();
                this.Error = (ErrorCode) source.ReadInt16();
                this.TaggedFields = source.ReadTaggedFields();
            }
        }
    }
}
