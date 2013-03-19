using System;
using System.Linq;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Bugs
{
	public class Issue25JavaBigDecimalBinaryConversion : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup(cqlVersion: CqlVersion.Cql);
			_db = setup.DB;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		[Fact]
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
			Assert.Equal(1000000000000000000M, (decimal)results.First(x => x.Key == "Key0")["MyColumn"]);
			Assert.Equal(.25M, (decimal)results.First(x => x.Key == "Key1")["MyColumn"]);
			Assert.Equal(2000000000000.1234M, (decimal)results.First(x => x.Key == "Key2")["MyColumn"]);
			Assert.Equal(-.25M, (decimal)results.First(x => x.Key == "Key3")["MyColumn"]);
		}
	}
}
