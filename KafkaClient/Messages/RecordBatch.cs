namespace KafkaClient
{
    using System;
    using System.Buffers;
    using System.IO;
    using System.Runtime.CompilerServices;
    using System.Text;

    public class RecordBatch : IRequest, IResponse
    {
        public long BaseOffset { get; set; }

        public int BatchLength { get; private set; }

        public int PartitionLeaderEpoch { get; private set; } = 0;

        public byte Magic { get; private set; } = 2;

        public int Crc { get; private set; }

        public short Attributes { get; private set; } = 0;

        public int LastOffsetDelta { get; set; }

        public long FirstTimestamp { get; set; }

        public long MaxTimestamp { get; set; }

        public long ProducerId { get; private set; } = -1;

        public short ProducerEpoch { get; private set; } = -1;

        public int BaseSequence { get; private set; } = -1;

        public Record[] Records { get; set; } = Array.Empty<Record>();

        public void Write(Stream destination)
        {
            using var crcSlice = MemoryStreamFactory.GetStream();
            crcSlice.WriteInt16(this.Attributes);
            crcSlice.WriteInt32(this.LastOffsetDelta);
            crcSlice.WriteInt64(this.FirstTimestamp);
            crcSlice.WriteInt64(this.MaxTimestamp);
            crcSlice.WriteInt64(this.ProducerId);
            crcSlice.WriteInt16(this.ProducerEpoch);
            crcSlice.WriteInt32(this.BaseSequence);
            crcSlice.WriteArray(this.Records);

            var crcSliceLength = (int) crcSlice.Length;
            this.Crc = (int) Crc32CHash.Compute(crcSlice.GetBuffer(), 0, crcSliceLength);

            destination.WriteInt32(crcSliceLength + 8 + 4 + 4 + 1 + 4);
            destination.WriteInt64(this.BaseOffset);
            destination.WriteInt32(this.BatchLength = GetBatchSizeFromCrcSliceSize(crcSliceLength));
            destination.WriteInt32(this.PartitionLeaderEpoch);
            destination.WriteByte(this.Magic);
            destination.WriteInt32(this.Crc);
            crcSlice.WriteTo(destination);
        }

        public void Read(Stream source)
        {
            var size = source.ReadInt32();

            if (size == 0)
                return;

            using var tracked = new TrackedStream(source, size);
            this.BaseOffset = tracked.ReadInt64();
            this.BatchLength = tracked.ReadInt32();
            this.PartitionLeaderEpoch = tracked.ReadInt32();
            this.Magic = (byte) tracked.ReadByte();
            this.Crc = tracked.ReadInt32();
            this.Attributes = tracked.ReadInt16();
            this.LastOffsetDelta = tracked.ReadInt32();
            this.FirstTimestamp = tracked.ReadInt64();
            this.MaxTimestamp = tracked.ReadInt64();
            this.ProducerId = tracked.ReadInt64();
            this.ProducerEpoch = tracked.ReadInt16();
            this.BaseSequence = tracked.ReadInt32();
            this.Records = tracked.ReadArray<Record>();
            tracked.DiscardRemainingData();

            // The code below calculates the CRC32c
            // var size = source.ReadInt32();
            //
            // if (size == 0)
            //     return;
            //
            // var data = ArrayPool<byte>.Shared.Rent(size);
            //
            // try
            // {
            //     source.Read(data, 0, size);
            //     using var tmp = new MemoryStream(data, 0, size);
            //     this.BaseOffset = tmp.ReadInt64();
            //     this.BatchLength = tmp.ReadInt32();
            //     this.PartitionLeaderEpoch = tmp.ReadInt32();
            //     this.Magic = (byte) tmp.ReadByte();
            //     this.Crc = tmp.ReadInt32();
            //
            //     var crc = (int) Crc32CHash.Compute(
            //         data,
            //         (int)tmp.Position,
            //         this.BatchLength - 4 - 1 - 4);
            //
            //     if (crc != this.Crc)
            //     {
            //         throw new Exception("Corrupt message");
            //     }
            //
            //     this.Attributes = tmp.ReadInt16();
            //     this.LastOffsetDelta = tmp.ReadInt32();
            //     this.FirstTimestamp = tmp.ReadInt64();
            //     this.MaxTimestamp = tmp.ReadInt64();
            //     this.ProducerId = tmp.ReadInt64();
            //     this.ProducerEpoch = tmp.ReadInt16();
            //     this.BaseSequence = tmp.ReadInt32();
            //     this.Records = tmp.ReadArray<Record>();
            // }
            // finally
            // {
            //     ArrayPool<byte>.Shared.Return(data);
            // }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetBatchSizeFromCrcSliceSize(int crcSliceSize)
        {
            return crcSliceSize +
                   4 + // Size of PartitionLeaderEpoch
                   1 + // Size of Magic
                   4; // Size of crc
        }

        public class Record : IRequest, IResponse
        {
            public int Length { get; private set; }

            public byte Attributes { get; private set; } = 0;

            public int TimestampDelta { get; set; }

            public int OffsetDelta { get; set; }

            public byte[] Key { get; set; }

            public byte[] Value { get; set; }

            public Header[] Headers { get; set; } = Array.Empty<Header>();

            public void Write(Stream destination)
            {
                using var tmp = MemoryStreamFactory.GetStream();

                tmp.WriteByte(this.Attributes);
                tmp.WriteVarint(this.TimestampDelta);
                tmp.WriteVarint(this.OffsetDelta);

                if (this.Key is null)
                {
                    tmp.WriteVarint(-1);
                }
                else
                {
                    tmp.WriteVarint(this.Key.Length);
                    tmp.Write(this.Key);
                }

                if (this.Value is null)
                {
                    tmp.WriteVarint(-1);
                }
                else
                {
                    tmp.WriteVarint(this.Value.Length);
                    tmp.Write(this.Value);
                }

                tmp.WriteVarint(this.Headers.Length);
                foreach (var header in this.Headers)
                {
                    tmp.WriteMessage(header);
                }

                destination.WriteVarint(this.Length = Convert.ToInt32(tmp.Length));

                tmp.Position = 0;
                tmp.CopyTo(destination);
            }

            public void Read(Stream source)
            {
                this.Length = source.ReadVarint();
                this.Attributes = (byte) source.ReadByte();
                this.TimestampDelta = source.ReadVarint();
                this.OffsetDelta = source.ReadVarint();
                this.Key = source.ReadBytes(source.ReadVarint());
                this.Value = source.ReadBytes(source.ReadVarint());
                this.Headers = source.ReadArray<Header>(source.ReadVarint());
            }
        }

        public class Header : IRequest, IResponse
        {
            public string Key { get; set; }

            public byte[] Value { get; set; }

            public void Write(Stream destination)
            {
                var keyBytes = Encoding.UTF8.GetBytes(this.Key);

                destination.WriteVarint(keyBytes.Length);
                destination.Write(keyBytes);

                if (this.Value is null)
                {
                    destination.WriteVarint(-1);
                }
                else
                {
                    destination.WriteVarint(this.Value.Length);
                    destination.Write(this.Value);
                }
            }

            public void Read(Stream source)
            {
                this.Key = source.ReadString(source.ReadVarint());
                this.Value = source.ReadBytes(source.ReadVarint());
            }
        }
    }
}
