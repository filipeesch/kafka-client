namespace KafkaClient
{
    using System;
    using System.Buffers.Binary;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.CompilerServices;
    using System.Text;

    public static class StreamExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteMessage(this Stream destination, IRequest message) => message.Write(destination);

        public static void WriteInt16(this Stream destination, short value)
        {
            Span<byte> tmp = stackalloc byte[2];
            BinaryPrimitives.WriteInt16BigEndian(tmp, value);
            destination.Write(tmp);
        }

        public static void WriteInt32(this Stream destination, int value)
        {
            Span<byte> tmp = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(tmp, value);
            destination.Write(tmp);
        }

        public static void WriteInt64(this Stream destination, long value)
        {
            Span<byte> tmp = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(tmp, value);
            destination.Write(tmp);
        }

        public static void WriteInt32Array(this Stream destination, IReadOnlyCollection<int> values)
        {
            destination.WriteInt32(values.Count);

            foreach (var value in values)
            {
                destination.WriteInt32(value);
            }
        }

        public static void WriteInt32CompactArray(this Stream destination, IReadOnlyCollection<int> values)
        {
            destination.WriteUVarint((uint) values.Count);

            foreach (var value in values)
            {
                destination.WriteInt32(value);
            }
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

        public static void WriteCompactString(this Stream destination, string value)
        {
            if (value is null)
            {
                destination.WriteUVarint(0);
                return;
            }

            destination.WriteUVarint((uint) value.Length + 1u);
            destination.Write(Encoding.UTF8.GetBytes(value));
        }

        public static void WriteBoolean(this Stream destination, bool value)
        {
            destination.WriteByte((byte) (value ? 1 : 0));
        }

        public static void WriteArray<TMessage>(this Stream destination, TMessage[] items)
            where TMessage : IRequest
        {
            destination.WriteInt32(items.Length);

            foreach (var item in items)
            {
                destination.WriteMessage(item);
            }
        }

        public static void WriteCompactArray<TMessage>(this Stream destination, TMessage[] items)
            where TMessage : IRequest
        {
            destination.WriteUVarint((uint) items.Length + 1);

            foreach (var item in items)
            {
                destination.WriteMessage(item);
            }
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

        public static byte[] ReadBytes(this Stream source, int count)
        {
            var bytes = new byte[count];
            source.Read(bytes);
            return bytes;
        }

        public static string ReadString(this Stream source)
        {
            return source.ReadString(source.ReadInt16());
        }

        public static string ReadString(this Stream source, int size)
        {
            if (size < 0)
            {
                return null;
            }

            Span<byte> buffer = stackalloc byte[size];
            source.Read(buffer);
            return Encoding.UTF8.GetString(buffer);
        }

        public static string ReadCompactString(this Stream source)
        {
            var size = (int) source.ReadUVarint();

            if (size <= 0)
            {
                return null;
            }

            Span<byte> buffer = stackalloc byte[size - 1];
            source.Read(buffer);
            return Encoding.UTF8.GetString(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TMessage ReadMessage<TMessage>(this Stream source)
            where TMessage : class, IResponse, new()
        {
            var message = new TMessage();
            message.Read(source);
            return message;
        }

        public static TMessage[] ReadArray<TMessage>(this Stream source) where TMessage : class, IResponse, new() =>
            source.ReadMany<TMessage>(source.ReadInt32());

        public static TMessage[] ReadMany<TMessage>(this Stream source, int count) where TMessage : class, IResponse, new()
        {
            if (count < 0)
                return null;

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

        public static int ReadVarint(this Stream source)
        {
            var num = source.ReadUVarint();

            return (int) (num >> 1) ^ -(int) (num & 1);
        }

        public static uint ReadUVarint(this Stream source)
        {
            const int endMask = 0b1000_0000;
            const int valueMask = 0b0111_1111;

            var num = 0;
            var shift = 0;
            int current;

            do
            {
                current = source.ReadByte();
                num |= (current & valueMask) << shift;
                shift += 7;
            } while ((current & endMask) != 0);

            return (uint) num;
        }

        public static void WriteVarint(this Stream destination, long num) =>
            destination.WriteUVarint(((ulong) num << 1) ^ ((ulong) num >> 63));

        public static void WriteUVarint(this Stream destination, ulong num)
        {
            const ulong endMask = 0b1000_0000;
            const ulong valueMask = 0b0111_1111;

            do
            {
                var value = (byte) ((num & valueMask) | (num > valueMask ? endMask : 0));
                destination.WriteByte(value);
                num >>= 7;
            } while (num != 0);
        }
    }
}
