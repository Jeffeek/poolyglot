// Identity-based dictionary key comparer (the pool tracks borrow state by object identity,
// just like Python's id()). The BCL ships ReferenceEqualityComparer only in .NET 5+.
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace ConnPool
{
    internal sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T> where T : class
    {
        public static readonly ReferenceEqualityComparer<T> Instance = new ReferenceEqualityComparer<T>();

        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);

        public int GetHashCode(T obj) => RuntimeHelpers.GetHashCode(obj);
    }
}
