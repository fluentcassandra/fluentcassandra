using System;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Integration.Tests
{
	
	public class ReportedIssuesTests
	{
		public const string FamilyName = "Standard";
		public const string TestKey = "Test1";

		[Fact]
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
