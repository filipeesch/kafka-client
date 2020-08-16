namespace KafkaClient
{
    using System;
    using System.IO;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Threading.Tasks;
    using Crc32C;

    public class RecordBatch : IRequest, IResponse
    {
        public long BaseOffset { get; set; }

        public int BatchLength { get; private set; }

        public int PartitionLeaderEpoch { get; private set; } = 0;

        public byte Magic { get; private set; } = 2;

        public int CRC { get; private set; }

        public byte[] Attributes { get; private set; } = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

        public int LastOffsetDelta { get; set; }

        public long FirstTimestamp { get; set; }

        public long MaxTimestamp { get; set; }

        public long ProducerId { get; private set; } = 0;

        public short ProducerEpoch { get; private set; } = 0;

        public int BaseSequence { get; private set; } = 0;

        public Record[] Records { get; set; } = Array.Empty<Record>();

        public void Write(Stream destination)
        {
            using var crcSlice = new MemoryStream(1024 * 4);
            crcSlice.Write(this.Attributes);
            crcSlice.WriteInt32(this.LastOffsetDelta);
            crcSlice.WriteInt64(this.FirstTimestamp);
            crcSlice.WriteInt64(this.MaxTimestamp);
            crcSlice.WriteInt64(this.ProducerId);
            crcSlice.WriteInt16(this.ProducerEpoch);
            crcSlice.WriteInt32(this.BaseSequence);

            crcSlice.WriteInt32(this.Records.Length);
            foreach (var record in this.Records)
            {
                crcSlice.WriteMessage(record);
            }

            var crcBytes = crcSlice.ToArray();

            this.CRC = (int) Crc32CAlgorithm.Compute(crcBytes);

            destination.WriteInt32(crcBytes.Length + 8 + 4 + 4 + 1 + 4);
            destination.WriteInt64(this.BaseOffset);
            destination.WriteInt32(this.BatchLength = GetBatchSizeFromCrcSliceSize(crcBytes.Length));
            destination.WriteInt32(this.PartitionLeaderEpoch);
            destination.WriteByte(this.Magic);
            destination.WriteInt32(this.CRC);
            destination.Write(crcBytes);
        }

        public void Read(Stream source)
        {
            var size = source.ReadInt32();

            if (size == 0)
                return;

            var data = source.ReadBytes(size);

            using var tmp = new MemoryStream(data);
            this.BaseOffset = tmp.ReadInt64();
            this.BatchLength = tmp.ReadInt32();
            this.PartitionLeaderEpoch = tmp.ReadInt32();
            this.Magic = (byte) tmp.ReadByte();
            this.CRC = tmp.ReadInt32();

            var crc = Crc32CAlgorithm.Compute(
                data,
                8 + 4 + 4 + 1 + 4,
                this.BatchLength - 4 - 1 - 4);

            if (crc != this.CRC)
            {
                throw new Exception("Corrupt message");
            }

            this.Attributes = tmp.ReadBytes(2);
            this.LastOffsetDelta = tmp.ReadInt32();
            this.FirstTimestamp = tmp.ReadInt64();
            this.MaxTimestamp = tmp.ReadInt64();
            this.ProducerId = tmp.ReadInt64();
            this.ProducerEpoch = tmp.ReadInt16();
            this.BaseSequence = tmp.ReadInt32();
            this.Records = tmp.ReadMany<Record>();
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

            public byte Attributes { get; set; } = 0;

            public int TimestampDelta { get; set; }

            public int OffsetDelta { get; set; }

            public byte[] Key { get; set; }

            public byte[] Value { get; set; }

            public Header[] Headers { get; set; } = Array.Empty<Header>();

            public void Write(Stream destination)
            {
                using var tmp = new MemoryStream(1024);

                tmp.WriteInt16(this.Attributes);
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
                this.Headers = source.ReadMany<Header>(source.ReadVarint());
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
