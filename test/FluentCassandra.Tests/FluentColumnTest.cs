using System;
using System.Linq;
using Xunit;
using FluentCassandra.Types;

namespace FluentCassandra
{
	
	public class FluentColumnTest
	{
		[Fact]
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
			Assert.Equal(nameExpected, (string)actual.ColumnName);
			Assert.Equal(valueExpected, (double)actual.ColumnValue);
			Assert.Equal(timestampExpected, actual.ColumnTimestamp.LocalDateTime.Date);
		}
	}
}
