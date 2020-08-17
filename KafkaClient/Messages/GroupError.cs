namespace KafkaClient.Messages
{
    public enum GroupError : short
    {
        IllegalGeneration = 22,
        InconsistentGroupProtocol = 23,
        InvalidGroupId = 24,
        UnknownMemberId = 25,
        InvalidSessionTimeout = 26,
        RebalanceInProgress = 27,
        MemberIdRequired = 79,
    }
}
