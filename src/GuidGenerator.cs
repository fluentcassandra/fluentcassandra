using System;
using System.Diagnostics;

namespace FluentCassandra
{
	public static class GuidGenerator
	{
		// number of bytes in guid
		public const int ByteArraySize = 16;

		// multiplex variant info
		public const int VariantByte = 8;
		public const int VariantByteMask = 0x3f;
		public const int VariantByteShift = 0x80;

		// multiplex version info
		public const int VersionByte = 7;
		public const int VersionByteMask = 0x0f;
		public const int VersionByteShift = 4;

		// indexes within the uuid array for certain boundaries
		private const byte TimestampByte = 0;
		private const byte GuidClockSequenceByte = 8;
		private const byte NodeByte = 10;

		// offset to move from 1/1/0001, which is 0-time for .NET, to gregorian 0-time of 10/15/1582
		private static readonly DateTimeOffset GregorianCalendarStart = new DateTimeOffset(1582, 10, 15, 0, 0, 0, TimeSpan.Zero);

		// random node that is 16 bytes
		private static readonly byte[] RandomNode;

		static GuidGenerator()
		{
			RandomNode = new byte[6];

			var random = new Random();
			random.NextBytes(RandomNode);
		}

		public static GuidVersion GetVersion(this Guid guid)
		{
			byte[] bytes = guid.ToByteArray();
			return (GuidVersion)((bytes[VersionByte] & 0xFF) >> VersionByteShift);
		}

		public static DateTimeOffset GetDateTimeOffset(Guid guid)
		{
			byte[] bytes = guid.ToByteArray();

			// reverse the version
			bytes[VersionByte] &= (byte)VersionByteMask;
			bytes[VersionByte] |= (byte)((byte)GuidVersion.TimeBased >> VersionByteShift);

			byte[] timestampBytes = new byte[8];
			Array.Copy(bytes, TimestampByte, timestampBytes, 0, 8);

			long timestamp = BitConverter.ToInt64(timestampBytes, 0);
			long ticks = timestamp + GregorianCalendarStart.Ticks;

			return new DateTimeOffset(ticks, TimeSpan.Zero);
		}

		public static DateTime GetDateTime(Guid guid)
		{
			return GetDateTimeOffset(guid).DateTime;
		}

		public static DateTime GetLocalDateTime(Guid guid)
		{
			return GetDateTimeOffset(guid).LocalDateTime;
		}

		public static DateTime GetUtcDateTime(Guid guid)
		{
			return GetDateTimeOffset(guid).UtcDateTime;
		}

		public static Guid GenerateTimeBasedGuid()
		{
            return GenerateTimeBasedGuid(new DateTimePrecise(5).UtcNow, RandomNode);
		}

		public static Guid GenerateTimeBasedGuid(DateTime dateTime)
		{
			return GenerateTimeBasedGuid(dateTime, RandomNode);
		}

		public static Guid GenerateTimeBasedGuid(DateTimeOffset dateTime)
		{
			return GenerateTimeBasedGuid(dateTime, RandomNode);
		}

		public static Guid GenerateTimeBasedGuid(DateTime dateTime, byte[] node)
		{
			return GenerateTimeBasedGuid(new DateTimeOffset(dateTime), node);
		}

		public static Guid GenerateTimeBasedGuid(DateTimeOffset dateTime, byte[] node)
		{
			long ticks = (dateTime - GregorianCalendarStart).Ticks;
			byte[] guid = new byte[ByteArraySize];
			byte[] clockSequenceBytes = BitConverter.GetBytes(Convert.ToInt16(Environment.TickCount % Int16.MaxValue));
			byte[] timestamp = BitConverter.GetBytes(ticks);

			// copy node
			Array.Copy(node, 0, guid, NodeByte, Math.Min(6, node.Length));

			// copy clock sequence
			Array.Copy(clockSequenceBytes, 0, guid, GuidClockSequenceByte, Math.Min(2, clockSequenceBytes.Length));

			// copy timestamp
			Array.Copy(timestamp, 0, guid, TimestampByte, Math.Min(8, timestamp.Length));

			// set the variant
			guid[VariantByte] &= (byte)VariantByteMask;
			guid[VariantByte] |= (byte)VariantByteShift;

			// set the version
			guid[VersionByte] &= (byte)VersionByteMask;
			guid[VersionByte] |= (byte)((byte)GuidVersion.TimeBased << VersionByteShift);

			return new Guid(guid);
		}

        /// DateTimePrecise class in C# -- an improvement to DateTime.Now
        /// By jamesdbrock
        /// http://www.codeproject.com/KB/cs/DateTimePrecise.aspx
        /// Licensed via The Code Project Open License (CPOL) 1.02
        /// http://www.codeproject.com/info/cpol10.aspx
        /// 
        /// DateTimePrecise provides a way to get a DateTime that exhibits the
        /// relative precision of
        /// System.Diagnostics.Stopwatch, and the absolute accuracy of DateTime.Now.
        private class DateTimePrecise
        {
            /// Creates a new instance of DateTimePrecise.
            /// A large value of synchronizePeriodSeconds may cause arithmetic overthrow
            /// exceptions to be thrown. A small value may cause the time to be unstable.
            /// A good value is 10.
            /// synchronizePeriodSeconds = The number of seconds after which the
            /// DateTimePrecise will synchronize itself with the system clock.
            public DateTimePrecise(long synchronizePeriodSeconds)
            {
                Stopwatch = Stopwatch.StartNew();
                this.Stopwatch.Start();

                DateTime t = DateTime.UtcNow;
                _immutable = new DateTimePreciseSafeImmutable(t, t, Stopwatch.ElapsedTicks,
                    Stopwatch.Frequency);

                _synchronizePeriodSeconds = synchronizePeriodSeconds;
                _synchronizePeriodStopwatchTicks = synchronizePeriodSeconds *
                    Stopwatch.Frequency;
                _synchronizePeriodClockTicks = synchronizePeriodSeconds *
                    _clockTickFrequency;
            }

            /// Returns the current date and time, just like DateTime.UtcNow.
            public DateTime UtcNow
            {
                get
                {
                    long s = this.Stopwatch.ElapsedTicks;
                    DateTimePreciseSafeImmutable immutable = _immutable;

                    if (s < immutable._s_observed + _synchronizePeriodStopwatchTicks)
                    {
                        return immutable._t_base.AddTicks(((
                            s - immutable._s_observed) * _clockTickFrequency) / (
                            immutable._stopWatchFrequency));
                    }
                    else
                    {
                        DateTime t = DateTime.UtcNow;

                        DateTime t_base_new = immutable._t_base.AddTicks(((
                            s - immutable._s_observed) * _clockTickFrequency) / (
                            immutable._stopWatchFrequency));

                        _immutable = new DateTimePreciseSafeImmutable(
                            t,
                            t_base_new,
                            s,
                            ((s - immutable._s_observed) * _clockTickFrequency * 2)
                            /
                            (t.Ticks - immutable._t_observed.Ticks + t.Ticks +
                                t.Ticks - t_base_new.Ticks - immutable._t_observed.Ticks)
                        );

                        return t_base_new;
                    }
                }
            }

            /// Returns the current date and time, just like DateTime.Now.
            public DateTime Now
            {
                get
                {
                    return this.UtcNow.ToLocalTime();
                }
            }

            /// The internal System.Diagnostics.Stopwatch used by this instance.
            public Stopwatch Stopwatch;

            private long _synchronizePeriodStopwatchTicks;
            private long _synchronizePeriodSeconds;
            private long _synchronizePeriodClockTicks;
            private const long _clockTickFrequency = 10000000;
            private DateTimePreciseSafeImmutable _immutable;
        }

        internal sealed class DateTimePreciseSafeImmutable
        {
            internal DateTimePreciseSafeImmutable(DateTime t_observed, DateTime t_base,
                 long s_observed, long stopWatchFrequency)
            {
                _t_observed = t_observed;
                _t_base = t_base;
                _s_observed = s_observed;
                _stopWatchFrequency = stopWatchFrequency;
            }
            internal readonly DateTime _t_observed;
            internal readonly DateTime _t_base;
            internal readonly long _s_observed;
            internal readonly long _stopWatchFrequency;
        }
	}
}