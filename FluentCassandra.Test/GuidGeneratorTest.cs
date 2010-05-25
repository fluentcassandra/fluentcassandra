using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FluentCassandra.Test
{
	[TestClass]
	public class GuidGeneratorTest
	{
		[TestMethod]
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

		[TestMethod]
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

		[TestMethod]
		public void GetDateTime()
		{
			// arrange
			var expected = new DateTime(1980, 3, 14, 12, 23, 42, 112);
			var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

			// act
			var actual = GuidGenerator.GetDateTime(guid);

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
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
