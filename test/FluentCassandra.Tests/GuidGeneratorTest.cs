using System;
using System.Linq;
using Xunit;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace FluentCassandra
{
	
	public class GuidGeneratorTest
	{
		[Fact]
		public void Type1Check()
		{
			// arrange
			var expected = GuidVersion.TimeBased;
			var guid = GuidGenerator.GenerateTimeBasedGuid();

			// act
			var actual = guid.GetVersion();

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void SanityType1Check()
		{
			// arrange
			var expected = GuidVersion.TimeBased;
			var guid = Guid.NewGuid();

			// act
			var actual = guid.GetVersion();

			// assert
			Assert.NotEqual(expected, actual);
		}

		[Fact]
		public void GetDateTimeUnspecified()
		{
			// arrange
			var expected = new DateTime(1980, 3, 14, 12, 23, 42, 112, DateTimeKind.Unspecified);
			var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

			// act
			var actual = GuidGenerator.GetDateTime(guid).ToLocalTime();

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void GetDateTimeLocal()
		{
			// arrange
			var expected = new DateTime(1980, 3, 14, 12, 23, 42, 112, DateTimeKind.Local);
			var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

			// act
			var actual = GuidGenerator.GetLocalDateTime(guid);

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void GetDateTimeUtc()
		{
			// arrange
			var expected = new DateTime(1980, 3, 14, 12, 23, 42, 112, DateTimeKind.Utc);
			var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

			// act
			var actual = GuidGenerator.GetUtcDateTime(guid);

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void GetDateTimeOffset()
		{
			// arrange
			var expected = new DateTimeOffset(1980, 3, 14, 12, 23, 42, 112, TimeSpan.Zero);
			var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

			// act
			var actual = GuidGenerator.GetDateTimeOffset(guid);

			// assert
			Assert.Equal(expected, actual);
		}

        [Fact]
        public void CheckGuidGenerationPararell()
        {
            ConcurrentBag<Guid> guidList = new ConcurrentBag<Guid>();
            List<int> list = new List<int>();
            for (int i = 0; i < 10; i++)
            {
                list.Add(i);
            }
            list.AsParallel().ForAll(index => guidList.Add(GuidGenerator.GenerateTimeBasedGuid()));

            var expected = 10;
            var actual = guidList.Count;
            // assert
            Assert.Equal(expected, actual);
        }
	}
}
