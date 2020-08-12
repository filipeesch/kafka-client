namespace KafkaClient
{
    using System;
    using System.Buffers.Binary;
    using System.IO;
    using System.Text;

    public static class StreamExtensions
    {
        public static void WriteMessage(this Stream destination, IRequest message) => message.Write(destination);

        public static void WriteInt16(this Stream destination, short value)
        {
            var tmp = new byte[2];
            BinaryPrimitives.WriteInt16BigEndian(tmp, value);
            destination.Write(tmp);
        }

        public static void WriteInt32(this Stream destination, int value)
        {
            var tmp = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(tmp, value);
            destination.Write(tmp);
        }

        public static void WriteInt64(this Stream destination, long value)
        {
            var tmp = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(tmp, value);
            destination.Write(tmp);
        }

        public static void WriteString(this Stream destination, string value)
        {
            if (value is null)
            {
                destination.WriteInt16(-1);
                return;
            }

            destination.WriteInt16(Convert.ToInt16(value.Length));
            destination.Write(Encoding.UTF8.GetBytes(value));
        }

        public static bool ReadBoolean(this Stream source) => source.ReadByte() != 0;

        public static short ReadInt16(this Stream source)
        {
            Span<byte> buffer = stackalloc byte[2];
            source.Read(buffer);
            return BinaryPrimitives.ReadInt16BigEndian(buffer);
        }

        public static int ReadInt32(this Stream source)
        {
            Span<byte> buffer = stackalloc byte[4];
            source.Read(buffer);
            return BinaryPrimitives.ReadInt32BigEndian(buffer);
        }

        public static long ReadInt64(this Stream source)
        {
            Span<byte> buffer = stackalloc byte[8];
            source.Read(buffer);
            return BinaryPrimitives.ReadInt64BigEndian(buffer);
        }

        public static string ReadString(this Stream source)
        {
            var size = source.ReadInt16();

            if (size < 0)
            {
                return null;
            }

            Span<byte> buffer = stackalloc byte[size];
            source.Read(buffer);
            return Encoding.UTF8.GetString(buffer);
        }

        public static TMessage[] ReadMany<TMessage>(this Stream source) where TMessage : class, IResponseMessage, new()
        {
            var count = source.ReadInt32();
            var result = new TMessage[count];

            for (var i = 0; i < count; i++)
            {
                result[i] = new TMessage();
                result[i].Read(source);
            }

            return result;
        }

        public static int[] ReadManyInt32(this Stream source)
        {
            var count = source.ReadInt32();
            var result = new int[count];

            for (var i = 0; i < count; i++)
            {
                result[i] = source.ReadInt32();
            }

            return result;
        }
    }
}
