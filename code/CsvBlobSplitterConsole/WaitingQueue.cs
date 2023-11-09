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
        #region Inner Types
        public record QueueResult(bool IsCompleted, T? Item);
        #endregion

        private readonly ConcurrentQueue<T> _queue = new();
        private bool _isCompleted = false;
        private volatile TaskCompletionSource _newItemSource = new();

        public bool HasData => _queue.Any();

        public void Enqueue(T item)
        {
            var taskCompletionSource = _newItemSource;

            _queue.Enqueue(item);
            Interlocked.Exchange(ref _newItemSource, new());
            taskCompletionSource.SetResult();
        }

        public async ValueTask<QueueResult> DequeueAsync()
        {
            var waitTask = _newItemSource.Task;

            if(_queue.TryDequeue(out var result))
            {
                return new QueueResult(true, result);
            }
            else if(_isCompleted && !_queue.Any())
            {
                return new QueueResult(false, default(T));
            }
            else
            {
                await waitTask;

                //  Recurse to actually dequeue the value
                return await DequeueAsync();
            }
        }

        public void Complete()
        {
            _isCompleted = true;
            _newItemSource.TrySetResult();
        }
    }
}