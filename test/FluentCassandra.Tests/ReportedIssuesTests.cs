using System;
using System.Linq;
using FluentCassandra.Types;
using NUnit.Framework;

namespace FluentCassandra
{
	[TestFixture]
	public class ReportedIssuesTests
	{
		public const string FamilyName = "Standard";
		public const string TestKey = "Test1";

		[Test]
		public void CreateRecord_Doesnt_Check_BytesType_Zero_Length()
		{
			// arrange
			var db = new CassandraContext("Test1", "localhost");
			var family = db.GetColumnFamily<AsciiType>(FamilyName);
			
			// act
			// assert
			Assert.Throws<ArgumentException>(delegate {
				dynamic value = family.CreateRecord("");
			});
		}
	}
}
