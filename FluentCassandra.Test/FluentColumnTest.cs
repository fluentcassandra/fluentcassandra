using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test
{
	[TestClass]
	public class FluentColumnTest
	{
		[TestMethod]
		public void Constructor_Test()
		{
			// arrange
			var nameExpected = "Test";
			var valueExpected = 300.0;
			var timestampExpected = DateTime.Today;

			// act
			var actual = new FluentColumn<AsciiType> {
				Name = nameExpected,
				Value = valueExpected
			};

			// assert
			Assert.AreEqual(nameExpected, (string)actual.Name);
			Assert.AreEqual(valueExpected, (double)actual.Value);
			Assert.AreEqual(timestampExpected, actual.Timestamp.LocalDateTime.Date);
		}
	}
}
