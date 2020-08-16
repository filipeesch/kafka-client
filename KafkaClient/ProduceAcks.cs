namespace KafkaClient
{
    public enum ProduceAcks : short
    {
        All = -1,
        None = 0,
        Leader = 1
    }
}
