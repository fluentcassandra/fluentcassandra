using System;
using System.Linq;
using NUnit.Framework;

namespace FluentCassandra
{
	[TestFixture]
	public class GuidGeneratorTest
	{
		[Test]
		public void Type1Check()
		{
			// arrange
			var expected = GuidVersion.TimeBased;
			var guid = GuidGenerator.GenerateTimeBasedGuid();

			// act
			var actual = guid.GetVersion();

			// assert
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void SanityType1Check()
		{
			// arrange
			var expected = GuidVersion.TimeBased;
			var guid = Guid.NewGuid();

			// act
			var actual = guid.GetVersion();

			// assert
			Assert.AreNotEqual(expected, actual);
		}

		[Test]
		public void GetDateTime()
		{
			// arrange
			var expected = new DateTime(1980, 3, 14, 12, 23, 42, 112, DateTimeKind.Utc);
			var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

			// act
			var actual = GuidGenerator.GetDateTime(guid);

			// assert
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void GetDateTimeOffset()
		{
			// arrange
			var expected = new DateTimeOffset(1980, 3, 14, 12, 23, 42, 112, TimeSpan.Zero);
			var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

			// act
			var actual = GuidGenerator.GetDateTimeOffset(guid);

			// assert
			Assert.AreEqual(expected, actual);
		}
	}
}
