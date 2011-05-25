using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra.Tests
{
	[TestFixture]
	public class FluentSuperColumnTest
	{
		[Test]
		public void Parent_Set()
		{
			// arrange

			// act
			var actual = new FluentSuperColumn<AsciiType, AsciiType>();

			// assert
			Assert.AreSame(actual, actual.GetPath().SuperColumn);
		}

		[Test]
		public void Path_Set()
		{
			// arrange

			// act
			var actual = new FluentSuperColumn<AsciiType, AsciiType>();

			// assert
			Assert.AreSame(actual, actual.GetPath().SuperColumn);
		}

		[Test]
		public void Constructor_Test()
		{
			// arrange
			var colName = "This is a test name";
			var col1 = new FluentColumn<AsciiType> { ColumnName = "Test1", ColumnValue = 300M };
			var col2 = new FluentColumn<AsciiType> { ColumnName = "Test2", ColumnValue = "Hello" };

			// act
			var actual = new FluentSuperColumn<AsciiType, AsciiType>();
			actual.ColumnName = colName;
			actual.Columns.Add(col1);
			actual.Columns.Add(col2);

			// assert
			Assert.AreEqual(colName, (string)actual.ColumnName);
			Assert.AreEqual(2, actual.Columns.Count);
		}

		[Test]
		public void Constructor_Dynamic_Test()
		{
			// arrange
			var colName = "This is a test name";
			var col1 = "Test1";
			var colValue1 = 300M;
			var col2 = "Test2";
			var colValue2 = "Hello";

			// act
			dynamic actual = new FluentSuperColumn<AsciiType, AsciiType>();
			actual.Name = colName;
			actual.Test1 = colValue1;
			actual.Test2 = colValue2;

			// assert
			Assert.AreEqual(colName, (string)actual.Name);
			//Assert.AreEqual(2, actual.Columns.Count);
			Assert.AreEqual(colValue1, (decimal)actual.Test1);
			Assert.AreEqual(colValue1, (decimal)actual[col1]);
			Assert.AreEqual(colValue2, (string)actual.Test2);
			Assert.AreEqual(colValue2, (string)actual[col2]);
		}

		[Test]
		public void Get_NonExistent_Column()
		{
			// arrange
			var colName = "This is a test name";
			var colValue1 = 300M;
			var colValue2 = "Hello";
			string expected = null;

			// act
			dynamic superColumn = new FluentSuperColumn<AsciiType, AsciiType>();
			superColumn.Name = colName;
			superColumn.Test1 = colValue1;
			superColumn.Test2 = colValue2;
			var actual = superColumn.Test3;

			// assert
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void Mutation()
		{
			// arrange
			var colName = "This is a test name";
			var col1 = new FluentColumn<AsciiType> { ColumnName = "Test1", ColumnValue = 300M };
			var col2 = new FluentColumn<AsciiType> { ColumnName = "Test2", ColumnValue = "Hello" };

			// act
			var actual = new FluentSuperColumn<AsciiType, AsciiType>();
			actual.ColumnName = colName;
			actual.Columns.Add(col1);
			actual.Columns.Add(col2);

			// assert
			var mutations = actual.MutationTracker.GetMutations();

			Assert.AreEqual(2, mutations.Count());
			Assert.AreEqual(2, mutations.Count(x => x.Type == MutationType.Added));

			var mut1 = mutations.FirstOrDefault(x => x.Column.ColumnName == "Test1");
			var mut2 = mutations.FirstOrDefault(x => x.Column.ColumnName == "Test2");

			Assert.AreSame(col1, mut1.Column);
			Assert.AreSame(col2, mut2.Column);

			Assert.AreSame(actual, mut1.Column.GetParent().SuperColumn);
			Assert.AreSame(actual, mut2.Column.GetParent().SuperColumn);
		}

		[Test]
		public void Dynamic_Mutation()
		{
			// arrange
			var colName = "This is a test name";
			var col1 = "Test1";
			var colValue1 = 300M;
			var col2 = "Test2";
			var colValue2 = "Hello";

			// act
			dynamic actual = new FluentSuperColumn<AsciiType, AsciiType>();
			actual.ColumnName = colName;
			actual.Test1 = colValue1;
			actual.Test2 = colValue2;

			// assert
			var mutations = ((IFluentRecord)actual).MutationTracker.GetMutations();

			Assert.AreEqual(2, mutations.Count());
			Assert.AreEqual(2, mutations.Count(x => x.Type == MutationType.Added));

			var mut1 = mutations.FirstOrDefault(x => x.Column.ColumnName == col1);
			var mut2 = mutations.FirstOrDefault(x => x.Column.ColumnName == col2);

			Assert.IsNotNull(mut1);
			Assert.IsNotNull(mut2);

			Assert.AreSame(actual, mut1.Column.GetParent().SuperColumn);
			Assert.AreSame(actual, mut2.Column.GetParent().SuperColumn);
		}
	}
}
