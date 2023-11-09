using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole
{
    internal class WaitingQueue<T>
    {
        private readonly ConcurrentQueue<T> _queue = new();
        private readonly TaskCompletionSource _completedSource = new();
        private volatile TaskCompletionSource _newItemSource = new();

        public Task CompletedTask => _completedSource.Task;

        public Task AwaitNewItemTask => _newItemSource.Task;

        public void Enqueue(T item)
        {
            var taskCompletionSource = _newItemSource;

            _queue.Enqueue(item);
            Interlocked.Exchange(ref _newItemSource, new());
            taskCompletionSource.SetResult();
        }

        public bool TryDequeue([MaybeNullWhen(false)] out T result)
            => _queue.TryDequeue(out result);

        public void Complete()
        {
            _completedSource.SetResult();
            _newItemSource.SetResult();
        }
    }
}