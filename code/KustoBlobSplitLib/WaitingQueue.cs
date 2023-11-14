using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib
{
    internal class WaitingQueue<T>
    {
        #region Inner Types
        public record QueueResult(bool IsCompleted, T? Item);
        #endregion

        private readonly ConcurrentQueue<T> _queue = new();
        private readonly TaskCompletionSource _isCompletedSource = new();
        private volatile TaskCompletionSource _newItemSource = new();
        private volatile int _consumerWaiting = 0;

        public bool HasData => _queue.Any();

        public bool IsConsumerWaiting => _consumerWaiting != 0;

        public void Enqueue(T item)
        {
            var oldSource = Interlocked.Exchange(
                ref _newItemSource,
                new());

            _queue.Enqueue(item);
            oldSource.SetResult();
        }

        public async ValueTask<QueueResult> DequeueAsync()
        {
            Interlocked.Increment(ref _consumerWaiting);

            try
            {
                var newItemTask = _newItemSource.Task;

                if (_queue.TryDequeue(out var result))
                {
                    return new QueueResult(false, result);
                }
                else if (_isCompletedSource.Task.IsCompleted && !_queue.Any())
                {
                    return new QueueResult(true, default(T));
                }
                else
                {
                    await Task.WhenAny(newItemTask, _isCompletedSource.Task);

                    //  Recurse to actually dequeue the value
                    return await DequeueAsync();
                }
            }
            finally
            {
                Interlocked.Decrement(ref _consumerWaiting);
            }
        }

        public void Complete()
        {
            _isCompletedSource.SetResult();
        }
    }
}