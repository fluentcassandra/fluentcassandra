using System;
using System.Linq;
using Xunit;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	
	public class CassandraIndexClauseTest
	{
		[Fact]
		public void StartKeySet()
		{
			// arrange
			string key = "test";
			int count = 20;
			string columnName = "test-column";
			string columnValue = "test-value";

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName] == columnValue);

			// assert
			Assert.Same((string)index.StartKey, key);
		}

		[Fact]
		public void CountSet()
		{
			// arrange
			string key = "test";
			int count = 20;
			string columnName = "test-column";
			string columnValue = "test-value";

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName] == columnValue);

			// assert
			Assert.Equal(index.Count, count);
		}

		[Fact]
		public void SingleExpression()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName = "test-column";
			var columnValue = "test-value";

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName] == columnValue);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.Equal(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.NotNull(firstExpression);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.Equal(firstExpression.Op, IndexOperator.EQ);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Fact]
		public void TwoExpressions()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName1 = "test1-column";
			var columnValue1 = "test1-value";
			var columnName2 = "test2-column";
			var columnValue2 = Math.PI;

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName1] == columnValue1
					&& family[columnName2] > columnValue2);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.Equal(2, expressions.Count);

			var firstExpression = expressions[0];
			Assert.NotNull(firstExpression);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName1);
			Assert.Equal(firstExpression.Op, IndexOperator.EQ);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue1);

			var secondExpression = expressions[1];
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(secondExpression.Column_name, typeof(BytesType)), (BytesType)columnName2);
			Assert.Equal(secondExpression.Op, IndexOperator.GT);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(secondExpression.Value, typeof(BytesType)), (BytesType)columnValue2);
		}

		[Fact]
		public void ThreeExpressions()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName1 = "test1-column";
			var columnValue1 = "test1-value";
			var columnName2 = "test2-column";
			var columnValue2 = Math.PI;
			var columnName3 = new DateTimeOffset(1980, 3, 14, 0, 0, 0, TimeSpan.Zero);
			var columnValue3 = Math.PI;

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName1] == columnValue1
					&& family[columnName2] > columnValue2
					&& family[columnName3] <= columnValue3);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.Equal(3, expressions.Count);

			var firstExpression = expressions[0];
			Assert.NotNull(firstExpression);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName1);
			Assert.Equal(firstExpression.Op, IndexOperator.EQ);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue1);

			var secondExpression = expressions[1];
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(secondExpression.Column_name, typeof(BytesType)), (BytesType)columnName2);
			Assert.Equal(secondExpression.Op, IndexOperator.GT);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(secondExpression.Value, typeof(BytesType)), (BytesType)columnValue2);

			var thirdExpression = expressions[2];
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(thirdExpression.Column_name, typeof(BytesType)), (BytesType)columnName3);
			Assert.Equal(secondExpression.Op, IndexOperator.GT);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(thirdExpression.Value, typeof(BytesType)), (BytesType)columnValue3);
		}

		[Fact]
		public void EqualsExpression()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName = "test-column";
			var columnValue = Math.PI;

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName] == columnValue);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.Equal(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.NotNull(firstExpression);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.Equal(firstExpression.Op, IndexOperator.EQ);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Fact]
		public void GreaterThanExpression()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName = "test-column";
			var columnValue = Math.PI;

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName] > columnValue);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.Equal(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.NotNull(firstExpression);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.Equal(firstExpression.Op, IndexOperator.GT);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Fact]
		public void GreaterThanOrEqualExpression()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName = "test-column";
			var columnValue = Math.PI;

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName] >= columnValue);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.Equal(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.NotNull(firstExpression);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.Equal(firstExpression.Op, IndexOperator.GTE);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Fact]
		public void LessThanExpression()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName = "test-column";
			var columnValue = Math.PI;

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName] < columnValue);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.Equal(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.NotNull(firstExpression);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.Equal(firstExpression.Op, IndexOperator.LT);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Fact]
		public void LessThanOrEqualExpression()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName = "test-column";
			var columnValue = Math.PI;

			// act
			var index = new CassandraIndexClause(
				key,
				count,
				family => family[columnName] <= columnValue);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.Equal(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.NotNull(firstExpression);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.Equal(firstExpression.Op, IndexOperator.LTE);
			Assert.Equal(CassandraObject.GetCassandraObjectFromDatabaseByteArray(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}
	}
}