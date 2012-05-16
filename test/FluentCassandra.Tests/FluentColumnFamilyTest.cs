using System;
using System.Linq;
using Xunit;
using FluentCassandra.Types;

namespace FluentCassandra
{
	
	public class FluentColumnFamilyTest
	{
		[Fact]
		public void Self_Set()
		{
			// arrange

			// act
			var actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");

			// assert
			Assert.Same(actual, actual.GetSelf().ColumnFamily);
		}

		[Fact]
		public void Path_Set()
		{
			// arrange

			// act
			var actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");

			// assert
			Assert.Same(actual, actual.GetPath().ColumnFamily);
		}

		[Fact]
		public void Constructor_Test()
		{
			// arrange
			var col1 = new FluentColumn<AsciiType> { ColumnName = "Test1", ColumnValue = 300M };
			var col2 = new FluentColumn<AsciiType> { ColumnName = "Test2", ColumnValue = "Hello" };

			// act
			var actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
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
			var col1 = "Test1";
			var colValue1 = 300M;
			var col2 = "Test2";
			var colValue2 = "Hello";

			// act
			dynamic actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			actual.Test1 = colValue1;
			actual.Test2 = colValue2;

			// assert
			Assert.Equal(colValue1, (decimal)actual.Test1);
			Assert.Equal(colValue1, (decimal)actual[col1]);
			Assert.Equal(colValue2, (string)actual.Test2);
			Assert.Equal(colValue2, (string)actual[col2]);
		}

		[Fact]
		public void Get_NonExistent_Column()
		{
			// arrange
			var colValue1 = 300M;
			var colValue2 = "Hello";
			var expected = (string)null;

			// act
			dynamic family = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			family.Test1 = colValue1;
			family.Test2 = colValue2;
			var actual = family.Test3;

			// assert
			Assert.Equal(expected, (string)actual);
		}

		[Fact]
		public void Mutation()
		{
			// arrange
			var col1 = new FluentColumn<AsciiType> { ColumnName = "Test1", ColumnValue = 300M };
			var col2 = new FluentColumn<AsciiType> { ColumnName = "Test2", ColumnValue = "Hello" };

			// act
			var actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
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
			var col1 = "Test1";
			var colValue1 = 300M;
			var col2 = "Test2";
			var colValue2 = "Hello";

			// act
			dynamic actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			actual.Test1 = colValue1;
			actual.Test2 = colValue2;

			// assert
			var mutations = ((IFluentRecord)actual).MutationTracker.GetMutations();

			Assert.Equal(2, mutations.Count());
			Assert.Equal(2, mutations.Count(x => x.Type == MutationType.Added));

			var mut1 = mutations.FirstOrDefault(x => x.Column.ColumnName == col1);
			var mut2 = mutations.FirstOrDefault(x => x.Column.ColumnName == col2);

			Assert.NotNull(mut1);
			Assert.NotNull(mut2);

			Assert.Same(actual, mut1.Column.GetParent().ColumnFamily);
			Assert.Same(actual, mut2.Column.GetParent().ColumnFamily);
		}

		[Fact]
		public void Mutation_Added()
		{
			// arrange
			var col1 = new FluentColumn<AsciiType> { ColumnName = "Test1", ColumnValue = 300M };
			var col2 = new FluentColumn<AsciiType> { ColumnName = "Test2", ColumnValue = "Hello" };

			// act
			var actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			actual.Columns.Add(col1);
			actual.Columns.Add(col2);

			// assert
			var mutations = actual.MutationTracker.GetMutations();

			Assert.Equal(2, mutations.Count());
			Assert.Equal(2, mutations.Count(x => x.Type == MutationType.Added));
		}

		[Fact]
		public void Mutation_Changed()
		{
			// arrange
			var col1 = new FluentColumn<AsciiType> { ColumnName = "Test1", ColumnValue = 300M };
			var col2 = new FluentColumn<AsciiType> { ColumnName = "Test1", ColumnValue = "Hello" };

			// act
			var actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			actual.Columns.Add(col1);
			actual.Columns[0] = col2;

			// assert
			var mutations = actual.MutationTracker.GetMutations().ToList();

			Assert.Equal(2, mutations.Count());
			Assert.Equal(MutationType.Added, mutations[0].Type);
			Assert.Equal(MutationType.Changed, mutations[1].Type);
		}


		[Fact]
		public void Mutation_Replaced()
		{
			// arrange
			var col1 = new FluentColumn<AsciiType> { ColumnName = "Test1", ColumnValue = 300M };
			var col2 = new FluentColumn<AsciiType> { ColumnName = "Test2", ColumnValue = "Hello" };

			// act
			var actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			actual.Columns.Add(col1);
			actual.Columns[0] = col2;

			// assert
			var mutations = actual.MutationTracker.GetMutations().ToList();

			Assert.Equal(3, mutations.Count());
			Assert.Equal(MutationType.Added, mutations[0].Type);
			Assert.Equal(MutationType.Removed, mutations[1].Type);
			Assert.Equal(MutationType.Added, mutations[2].Type);
		}

		[Fact]
		public void Mutation_Removed()
		{
			// arrange
			var col1 = new FluentColumn<AsciiType> { ColumnName = "Test1", ColumnValue = 300M };

			// act
			var actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			actual.Columns.Add(col1);
			actual.RemoveColumn("Test1");

			// assert
			var mutations = actual.MutationTracker.GetMutations().ToList();

			Assert.Equal(2, mutations.Count());
			Assert.Equal(MutationType.Added, mutations[0].Type);
			Assert.Equal(MutationType.Removed, mutations[1].Type);
		}

		[Fact]
		public void Dynamic_Mutation_Added()
		{
			// arrange
			var colValue1 = 300M;
			var colValue2 = "Hello";

			// act
			dynamic actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			actual.Test1 = colValue1;
			actual.Test2 = colValue2;

			// assert
			var mutations = ((IFluentRecord)actual).MutationTracker.GetMutations();

			Assert.Equal(2, mutations.Count());
			Assert.Equal(2, mutations.Count(x => x.Type == MutationType.Added));
		}

		[Fact]
		public void Dynamic_Mutation_Changed()
		{
			// arrange
			var colValue1 = 300M;
			var colValue2 = "Hello";

			// act
			dynamic actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			actual.Test1 = colValue1;
			actual.Test1 = colValue2;

			// assert
			var mutations = ((IFluentRecord)actual).MutationTracker.GetMutations().ToList();

			Assert.Equal(2, mutations.Count());
			Assert.Equal(MutationType.Added, mutations[0].Type);
			Assert.Equal(MutationType.Changed, mutations[1].Type);
		}

		[Fact]
		public void Dynamic_Mutation_Removed()
		{
			// arrange
			var colValue1 = 300M;

			// act
			dynamic actual = new FluentColumnFamily<AsciiType>("Keyspace1", "Standard1");
			actual.Test1 = colValue1;
			actual.RemoveColumn("Test1");

			// assert
			var mutations = ((IFluentRecord)actual).MutationTracker.GetMutations().ToList();

			Assert.Equal(2, mutations.Count());
			Assert.Equal(MutationType.Added, mutations[0].Type);
			Assert.Equal(MutationType.Removed, mutations[1].Type);
		}
	}
}
