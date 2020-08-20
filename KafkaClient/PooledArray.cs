namespace KafkaClient
{
    using System.Buffers;

    public readonly ref struct PooledArray<T>
    {
        private readonly ArrayPool<T> pool;
        private readonly T[] buffer;

        public PooledArray(int size, ArrayPool<T> pool)
        {
            this.pool = pool;
            this.buffer = pool.Rent(size);
        }

        public PooledArray(int size) : this(size, ArrayPool<T>.Shared)
        {
        }

        public T[] Buffer => this.buffer;

        public void Dispose()
        {
            this.pool.Return(this.buffer);
        }
    }
}
