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
			var expected = 1;
			var guid = GuidGenerator.GenerateTimeBasedGuid();

			// act
			var actual = GuidGenerator.GetGuidVersion(guid);

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void SanityType1Check()
		{
			// arrange
			var expected = 1;
			var guid = Guid.NewGuid();

			// act
			var actual = GuidGenerator.GetGuidVersion(guid);

			// assert
			Assert.AreNotEqual(expected, actual);
		}
	}
}
