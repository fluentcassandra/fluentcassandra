using System;
using System.Linq;
using Xunit;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	
	public class ColumnCountTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;
		private CassandraSuperColumnFamily<AsciiType, AsciiType> _superFamily;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup();
			_db = setup.DB;
			_family = setup.Family;
			_superFamily = setup.SuperFamily;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		private const string _testKey = "Test1";
		private const string _testSuperName = "SubTest1";

		[Fact]
		public void ColumnFamily_Key()
		{
			// arrange
			int expected = 3;

			// act
			int actual = _family.ColumnCount(_testKey, null, null);

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void SuperColumnFamily_Key()
		{
			// arrange
			int expected = 1;

			// act
			int actual = _superFamily.ColumnCount(_testKey, null, null);

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void SuperColumnFamily_Key_And_SuperColumnName()
		{
			// arrange
			int expected = 3;

			// act
			int actual = _superFamily.SuperColumnCount(_testKey, _testSuperName, null, null);

			// assert
			Assert.Equal(expected, actual);
		}
	}
}