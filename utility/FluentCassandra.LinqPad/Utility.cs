using System;
using System.Linq;

namespace FluentCassandra.LinqPad
{
    public static class Utility
    {
        public static bool IsNullOrWhitespace(this string source)
        {
            return source == null || source.Trim() == string.Empty;            
        }
    }
}
