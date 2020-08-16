namespace KafkaClient.Messages
{
    using System.IO;

    public class ProduceV8Request : IRequestMessage<ProduceV8Response>
    {
        public ProduceV8Request(ProduceAcks acks, int timeout, Topic[] topics)
        {
            this.Acks = acks;
            this.Timeout = timeout;
            this.Topics = topics;
        }

        public ApiKey ApiKey => ApiKey.Produce;

        public short ApiVersion => 8;

        public string TransactionalID { get; } = null;

        public ProduceAcks Acks { get; }

        public int Timeout { get; }

        public Topic[] Topics { get; }

        public void Write(Stream destination)
        {
            destination.WriteString(this.TransactionalID);
            destination.WriteInt16((short) this.Acks);
            destination.WriteInt32(this.Timeout);
            destination.WriteArray(this.Topics);
        }

        public class Topic : IRequest
        {
            public Topic(string name, Partition[] partitions)
            {
                this.Name = name;
                this.Partitions = partitions;
            }

            public string Name { get; }

            public Partition[] Partitions { get; }

            public void Write(Stream destination)
            {
                destination.WriteString(this.Name);
                destination.WriteArray(this.Partitions);
            }
        }

        public class Partition : IRequest
        {
            public Partition(int id, RecordBatch batch)
            {
                this.ID = id;
                this.Batch = batch;
            }

            public int ID { get; }

            public RecordBatch Batch { get; }

            public void Write(Stream destination)
            {
                destination.WriteInt32(this.ID);
                destination.WriteMessage(this.Batch);
            }
        }
    }
}
