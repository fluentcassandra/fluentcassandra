using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test.Types
{
	[TestClass]
	public class BytesTypeTest
	{
		[TestMethod]
		public void Double_To_BytesType()
		{
			// arrange
			double expected = Math.PI;
		
			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (double)actual);
		}
	}
}
