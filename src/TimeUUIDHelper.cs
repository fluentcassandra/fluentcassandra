using System;

namespace FluentCassandra
{
    public class TimeUUIDHelper
    {
        /// <summary>
        /// Allows for substitution for Unit Tests
        /// </summary>
        public static Func<DateTimeOffset> UtcNow = () => DateTimePrecise.UtcNowOffset ;

    }
}