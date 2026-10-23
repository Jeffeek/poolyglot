// Polyfill so `init`-only setters and records compile on netstandard2.1.
// The BCL type only ships in .NET 5+; this internal shim is erased by newer runtimes.
using System.ComponentModel;

namespace System.Runtime.CompilerServices
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    internal static class IsExternalInit
    {
    }
}
