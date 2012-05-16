using System;
using System.Linq;
using Xunit;
using FluentCassandra.Types;

namespace FluentCassandra
{
	
	public class FluentSuperColumnFamilyTest
	{
		[Fact]
		public void Self_Set()
		{
			// arrange

			// act
			var actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");

			// assert
			Assert.Same(actual, actual.GetSelf().ColumnFamily);
		}

		[Fact]
		public void Path_Set()
		{
			// arrange

			// act
			var actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");

			// assert
			Assert.Same(actual, actual.GetPath().ColumnFamily);
		}

		[Fact]
		public void Constructor_Test()
		{
			// arrange
			var col1 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test1" };
			var col2 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test2" };

			// act
			var actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");
			actual.Columns.Add(col1);
			actual.Columns.Add(col2);

			// assert
			Assert.Equal(2, actual.Columns.Count);
			Assert.Same(col1.Family, actual);
			Assert.Same(col2.Family, actual);
		}

		[Fact]
		public void Constructor_Dynamic_Test()
		{
			// arrange
			var col1 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test1" };
			var col2 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test2" };

			// act
			dynamic actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");
			actual.Test1 = col1;
			actual.Test2 = col2;

			// assert
			Assert.Equal(col1, actual.Test1);
			Assert.Equal(col1, actual["Test1"]);
			Assert.Equal(col2, actual.Test2);
			Assert.Equal(col2, actual["Test2"]);
			Assert.Same(col1.Family, actual);
			Assert.Same(col2.Family, actual);
		}

		[Fact]
		public void Get_NonExistent_Column()
		{
			// arrange
			var col1 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test1" };
			var col2 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test2" };

			// act
			dynamic family = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");
			family.Test1 = col1;
			family.Test2 = col2;
			var actual = family.Test3;

			// assert
			Assert.IsType(typeof(FluentSuperColumn), actual);
		}

		[Fact]
		public void Mutation()
		{
			// arrange
			var col1 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test1" };
			var col2 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test2" };

			// act
			var actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");
			actual.Columns.Add(col1);
			actual.Columns.Add(col2);

			// assert
			var mutations = actual.MutationTracker.GetMutations();

			Assert.Equal(2, mutations.Count());
			Assert.Equal(2, mutations.Count(x => x.Type == MutationType.Added));

			var mut1 = mutations.FirstOrDefault(x => x.Column.ColumnName == "Test1");
			var mut2 = mutations.FirstOrDefault(x => x.Column.ColumnName == "Test2");

			Assert.Same(col1, mut1.Column);
			Assert.Same(col2, mut2.Column);

			Assert.Same(actual, mut1.Column.GetParent().ColumnFamily);
			Assert.Same(actual, mut2.Column.GetParent().ColumnFamily);
		}

		[Fact]
		public void Dynamic_Mutation()
		{
			// arrange
			var col1 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test1" };
			var col2 = new FluentSuperColumn<AsciiType, AsciiType> { ColumnName = "Test2" };

			// act
			dynamic actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");
			actual.Test1 = col1;
			actual.Test2 = col2;

			// assert
			var mutations = ((IFluentRecord)actual).MutationTracker.GetMutations();

			Assert.Equal(2, mutations.Count());
			Assert.Equal(2, mutations.Count(x => x.Type == MutationType.Added));

			var mut1 = mutations.FirstOrDefault(x => x.Column.ColumnName == "Test1");
			var mut2 = mutations.FirstOrDefault(x => x.Column.ColumnName == "Test2");

			Assert.NotNull(mut1);
			Assert.NotNull(mut2);

			Assert.Same(actual, mut1.Column.GetParent().ColumnFamily);
			Assert.Same(actual, mut2.Column.GetParent().ColumnFamily);
		}
	}
}
