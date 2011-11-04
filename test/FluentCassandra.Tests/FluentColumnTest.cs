using System;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[TestFixture]
	public class FluentColumnTest
	{
		[Test]
		public void Constructor_Test()
		{
			// arrange
			var nameExpected = "Test";
			var valueExpected = 300.0;
			var timestampExpected = DateTime.Today;

			// act
			var actual = new FluentColumn<AsciiType> {
				ColumnName = nameExpected,
				ColumnValue = valueExpected
			};

			// assert
			Assert.AreEqual(nameExpected, (string)actual.ColumnName);
			Assert.AreEqual(valueExpected, (double)actual.ColumnValue);
			Assert.AreEqual(timestampExpected, actual.ColumnTimestamp.LocalDateTime.Date);
		}
	}
}
