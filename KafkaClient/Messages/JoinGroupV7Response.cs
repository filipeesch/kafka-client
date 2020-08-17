namespace KafkaClient.Messages
{
    using System.IO;

    public class JoinGroupV7Response : IResponseV2
    {
        public int ThrottleTimeMs { get; private set; }

        public GroupError Error { get; private set; }

        public int GenerationId { get; private set; }

        public string ProtocolType { get; private set; }

        public string ProtocolName { get; private set; }

        public string LeaderId { get; private set; }

        public string MemberId { get; private set; }

        public Member[] Members { get; private set; }

        public TaggedField[] TaggedFields { get; private set; }

        public void Read(Stream source)
        {
            this.ThrottleTimeMs = source.ReadInt32();
            this.Error = (GroupError) source.ReadInt16();
            this.GenerationId = source.ReadInt32();
            this.ProtocolType = source.ReadCompactString();
            this.ProtocolName = source.ReadCompactString();
            this.LeaderId = source.ReadCompactString();
            this.MemberId = source.ReadCompactString();
            this.Members = source.ReadCompactArray<Member>();
            this.TaggedFields = source.ReadTaggedFields();
        }

        public class Member : IResponseV2
        {
            public string MemberId { get; private set; }

            public string GroupInstanceId { get; private set; }

            public byte[] Metadata { get; private set; }

            public TaggedField[] TaggedFields { get; private set; }

            public void Read(Stream source)
            {
                this.MemberId = source.ReadCompactString();
                this.GroupInstanceId = source.ReadCompactString();
                this.Metadata = source.ReadCompactByteArray();
                this.TaggedFields = source.ReadTaggedFields();
            }
        }
    }
}
