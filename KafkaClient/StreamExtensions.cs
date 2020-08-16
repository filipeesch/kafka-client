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

        public static void WriteManyInt32(this Stream destination, IEnumerable<int> values)
        {
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

        public static void WriteMany<TMessage>(this Stream destination, TMessage[] items)
            where TMessage : IRequest
        {
            destination.WriteInt32(items.Length);

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TMessage ReadMessage<TMessage>(this Stream source)
            where TMessage : class, IResponse, new()
        {
            var message = new TMessage();
            message.Read(source);
            return message;
        }

        public static TMessage[] ReadMany<TMessage>(this Stream source) where TMessage : class, IResponse, new() =>
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

        // public static int ReadVarint(this Stream source)
        // {
        //     const int endMask = 0b1000_0000;
        //     const int valueMask = 0b0111_1111;
        //
        //     var shift = 0;
        //     var result = 0;
        //
        //     for (var i = 0; i < sizeof(int); i++)
        //     {
        //         var byteValue = source.ReadByte();
        //
        //         var value = byteValue & valueMask;
        //         result |= value << shift;
        //
        //         if ((byteValue & endMask) != endMask)
        //         {
        //             return result;
        //         }
        //
        //         shift += 7;
        //     }
        //
        //     return 0;
        // }
        public static int ReadVarint(this Stream source)
        {
            var num = 0;
            var shift = 0;
            int current;

            do
            {
                current = source.ReadByte();
                num |= (current & 0x7f) << shift;
                shift += 7;
            } while ((current & 0x80) != 0);

            return (num >> 1) ^ -(num & 1);
        }

        // public static void WriteVarint(this Stream destination, int value)
        // {
        //     const int endMask = 0b1000_0000;
        //     const int valueMask = 0b0111_1111;
        //     
        //     do
        //     {
        //         var byteVal = value & valueMask;
        //         value >>= 7;
        //
        //         if (value != 0)
        //         {
        //             byteVal |= endMask;
        //         }
        //
        //         destination.WriteByte((byte) byteVal);
        //     } while (value != 0);
        // }

        public static void WriteVarint(this Stream destination, long num)
        {
            const long endMask = 0b1000_0000;
            const long valueMask = 0b0111_1111;

            num = (num << 1) ^ (num >> 63);

            do
            {
                var value = (byte) ((num & valueMask) | (num > valueMask ? endMask : 0));
                destination.WriteByte(value);
                num >>= 7;
            } while (num != 0);
        }

        // public static void WriteVarint(this Stream destination, long num) =>
        //     destination.WriteVarint(Convert.ToUInt64((num << 1) ^ (num >> 63)));
    }
}
