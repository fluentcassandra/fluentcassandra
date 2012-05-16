using System;
using System.Linq;
using Xunit;
using FluentCassandra.Types;

namespace FluentCassandra
{
	
	public class CassandraQueryTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup();
			_db = setup.DB;
			_family = setup.Family;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		private const string _testKey = "Test1";
		private const string _testName = "Test1";

		[Fact]
		public void Query_Single_Column()
		{
			// arrange
			var expected = Math.PI;

			// act
			var actual = _family.Get(_testKey).FetchColumns(_testName).FirstOrDefault().AsDynamic().Test1;

			// assert
			Assert.Equal(expected, (double)actual);
		}

		[Fact]
		public void Query_Multi_Columns()
		{
			// arrange
			var expected = Math.PI;

			// act
			var actual = _family.Get(_testKey).FetchColumns("Test1", "Test2").FirstOrDefault().AsDynamic();

			// assert
			Assert.Equal(expected, (double)actual.Test1);
			Assert.Equal(expected, (double)actual.Test2);
		}

		[Fact]
		public void Query_Get_Two_Columns()
		{
			// arrange
			var expected = Math.PI;

			// act
			var actual = _family.Get(_testKey).StartWithColumn("Test1").TakeColumns(2).FirstOrDefault().AsDynamic();

			// assert
			Assert.Equal(expected, (double)actual.Test1);
			Assert.Equal(expected, (double)actual.Test2);
		}

		[Fact]
		public void Query_Get_Until_Test2_Column()
		{
			// arrange
			var expected = Math.PI;

			// act
			var actual = _family.Get(_testKey).StartWithColumn("Test1").TakeUntilColumn("Test2").FirstOrDefault().AsDynamic();

			// assert
			Assert.Equal(expected, (double)actual.Test1);
			Assert.Equal(expected, (double)actual.Test2);
		}

		[Fact]
		public void Query_Get_All_Columns()
		{
			// arrange
			var expectedCount = 3;

			// act
			var actual = _family.Get(_testKey).FirstOrDefault();

			// assert
			Assert.Equal(expectedCount, actual.Columns.Count);
			Assert.Equal("Test1", (string)actual.Columns[0].ColumnName);
			Assert.Equal("Test2", (string)actual.Columns[1].ColumnName);
			Assert.Equal("Test3", (string)actual.Columns[2].ColumnName);
		}

		[Fact]
		public void Query_Get_All_Columns_Reversed()
		{
			// arrange
			var expectedCount = 3;

			// act
			var actual = _family.Get(_testKey).ReverseColumns().FirstOrDefault();

			// assert
			Assert.Equal(expectedCount, actual.Columns.Count);
			Assert.Equal("Test3", (string)actual.Columns[0].ColumnName);
			Assert.Equal("Test2", (string)actual.Columns[1].ColumnName);
			Assert.Equal("Test1", (string)actual.Columns[2].ColumnName);
		}

		[Fact]
		public void Query_Get_All_Columns_DelayedLoading()
		{
			// arrange
			var expectedCount = 3;

			// act
			var actualNotLoaded = _family.Get(_testKey);

			// assert
			foreach (var actual in actualNotLoaded)
			{
				Assert.Equal(expectedCount, actual.Columns.Count);
				Assert.Equal("Test1", (string)actual.Columns[0].ColumnName);
				Assert.Equal("Test2", (string)actual.Columns[1].ColumnName);
				Assert.Equal("Test3", (string)actual.Columns[2].ColumnName);
			}
		}
	}
}