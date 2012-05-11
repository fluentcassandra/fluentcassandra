using System;
using System.Linq;
using NUnit.Framework;

namespace FluentCassandra.Bugs
{
	[TestFixture]
	public class Issue25JavaBigDecimalBinaryConversion
	{
		private CassandraContext _db;

		[TestFixtureSetUp]
		public void TestInit()
		{
			var setup = new CassandraDatabaseSetup();
			_db = setup.DB;
		}

		[Test]
		public void Test()
		{
			// arrange
			_db.ExecuteNonQuery("CREATE TABLE OfferReservation (KEY text PRIMARY KEY) WITH comparator = text AND default_validation = decimal");
			_db.ExecuteNonQuery("INSERT INTO OfferReservation (KEY, 'MyColumn') VALUES ('Key0', 1000000000000000000)");
			_db.ExecuteNonQuery("INSERT INTO OfferReservation (KEY, 'MyColumn') VALUES ('Key1', 0.25)");
			_db.ExecuteNonQuery("INSERT INTO OfferReservation (KEY, 'MyColumn') VALUES ('Key2', 2000000000000.1234)");
			_db.ExecuteNonQuery("INSERT INTO OfferReservation (KEY, 'MyColumn') VALUES ('Key3', -0.25)");

			// act
			var actual = _db.ExecuteQuery("SELECT * FROM OfferReservation");

			// assert
			var results = actual.ToList();
			Assert.AreEqual(1000000000000000000M, (decimal)results.First(x => x.Key == "Key0")["MyColumn"]);
			Assert.AreEqual(.25M, (decimal)results.First(x => x.Key == "Key1")["MyColumn"]);
			Assert.AreEqual(2000000000000.1234M, (decimal)results.First(x => x.Key == "Key2")["MyColumn"]);
			Assert.AreEqual(-.25M, (decimal)results.First(x => x.Key == "Key3")["MyColumn"]);
		}
	}
}
