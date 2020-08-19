namespace KafkaClient
{
    using System;
    using System.IO;
    using System.Runtime.CompilerServices;

    public class TrackedStream : Stream
    {
        private long position = 0;
        private readonly Stream stream;

        public TrackedStream(Stream stream)
        {
            this.stream = stream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Flush() => this.stream.Flush();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int Read(byte[] buffer, int offset, int count)
        {
            this.position += count;
            return this.stream.Read(buffer, offset, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override long Seek(long offset, SeekOrigin origin)
        {
            this.position += offset;
            return this.stream.Seek(offset, origin);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void SetLength(long value)
        {
            this.stream.SetLength(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Write(byte[] buffer, int offset, int count)
        {
            this.position += count;
            this.stream.Write(buffer, offset, count);
        }

        public override bool CanRead => this.stream.CanRead;
        public override bool CanSeek => this.stream.CanSeek;
        public override bool CanWrite => this.stream.CanWrite;
        public override long Length => this.stream.Length;

        public override long Position
        {
            get => this.position;
            set => throw new NotSupportedException();
        }
    }
}
