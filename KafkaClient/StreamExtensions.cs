namespace KafkaClient
{
    using System;
    using System.Buffers.Binary;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.CompilerServices;
    using System.Text;
    using KafkaClient.Messages;

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteEmptyTaggedFields(this Stream destination)
        {
            destination.WriteUVarint(0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipBytes(this Stream destination, int count)
        {
            while (--count >= 0)
            {
                destination.ReadByte();
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

            for (var i = 0; i < items.Length; ++i)
            {
                destination.WriteMessage(items[i]);
            }
        }

        public static void WriteTaggedFields(this Stream destination, TaggedField[] items)
        {
            destination.WriteUVarint((uint) items.Length);

            for (var i = 0; i < items.Length; ++i)
            {
                destination.WriteMessage(items[i]);
            }
        }

        public static void WriteCompactArray<TMessage>(this Stream destination, TMessage[] items)
            where TMessage : IRequest
        {
            destination.WriteUVarint((uint) items.Length + 1);

            for (var i = 0; i < items.Length; ++i)
            {
                destination.WriteMessage(items[i]);
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
            var size = source.ReadUVarint();

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TMessage[] ReadArray<TMessage>(this Stream source) where TMessage : class, IResponse, new() =>
            source.ReadArray<TMessage>(source.ReadInt32());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TMessage[] ReadCompactArray<TMessage>(this Stream source) where TMessage : class, IResponse, new() =>
            source.ReadArray<TMessage>(source.ReadUVarint() - 1);

        public static TMessage[] ReadArray<TMessage>(this Stream source, int count) where TMessage : class, IResponse, new()
        {
            if (count < 0)
                return null;

            if (count == 0)
                return Array.Empty<TMessage>();

            var result = new TMessage[count];

            for (var i = 0; i < count; i++)
            {
                result[i] = new TMessage();
                result[i].Read(source);
            }

            return result;
        }


        public static TaggedField[] ReadTaggedFields(this Stream source)
        {
            var count = source.ReadUVarint();

            if (count == 0)
                return Array.Empty<TaggedField>();

            var result = new TaggedField[count];

            for (var i = 0; i < count; i++)
            {
                result[i] = new TaggedField();
                result[i].Read(source);
            }

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int[] ReadInt32Array(this Stream source) => source.ReadInt32Array(source.ReadInt32());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int[] ReadInt32CompactArray(this Stream source) => source.ReadInt32Array(source.ReadUVarint() - 1);

        public static int[] ReadInt32Array(this Stream source, int count)
        {
            if (count < 0)
                return null;

            if (count == 0)
                return Array.Empty<int>();

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

            return (num >> 1) ^ -(num & 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadUVarint(this Stream source) => source.ReadUVarint(out _);

        public static int ReadUVarint(this Stream source, out int bytesReaded)
        {
            const int endMask = 0b1000_0000;
            const int valueMask = 0b0111_1111;

            bytesReaded = 0;

            var num = 0;
            var shift = 0;
            int current;

            do
            {
                current = source.ReadByte();

                if (++bytesReaded > 4)
                    throw new InvalidOperationException("The value is not a valid VARINT");

                num |= (current & valueMask) << shift;
                shift += 7;
            } while ((current & endMask) != 0);

            return num;
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
