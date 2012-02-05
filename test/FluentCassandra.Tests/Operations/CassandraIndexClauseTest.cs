using System;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	[TestFixture]
	public class CassandraIndexClauseTest
	{
		[Test]
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
			Assert.AreEqual(index.StartKey, key);
		}

		[Test]
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
			Assert.AreEqual(index.Count, count);
		}

		[Test]
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
			Assert.AreEqual(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.AreEqual(firstExpression.Op, IndexOperator.EQ);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Test]
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
			Assert.AreEqual(2, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName1);
			Assert.AreEqual(firstExpression.Op, IndexOperator.EQ);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue1);

			var secondExpression = expressions[1];
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(secondExpression.Column_name, typeof(BytesType)), (BytesType)columnName2);
			Assert.AreEqual(secondExpression.Op, IndexOperator.GT);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(secondExpression.Value, typeof(BytesType)), (BytesType)columnValue2);
		}

		[Test]
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
			Assert.AreEqual(3, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName1);
			Assert.AreEqual(firstExpression.Op, IndexOperator.EQ);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue1);

			var secondExpression = expressions[1];
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(secondExpression.Column_name, typeof(BytesType)), (BytesType)columnName2);
			Assert.AreEqual(secondExpression.Op, IndexOperator.GT);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(secondExpression.Value, typeof(BytesType)), (BytesType)columnValue2);

			var thirdExpression = expressions[2];
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(thirdExpression.Column_name, typeof(BytesType)), (BytesType)columnName3);
			Assert.AreEqual(secondExpression.Op, IndexOperator.GT);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(thirdExpression.Value, typeof(BytesType)), (BytesType)columnValue3);
		}

		[Test]
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
			Assert.AreEqual(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.AreEqual(firstExpression.Op, IndexOperator.EQ);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Test]
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
			Assert.AreEqual(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.AreEqual(firstExpression.Op, IndexOperator.GT);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Test]
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
			Assert.AreEqual(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.AreEqual(firstExpression.Op, IndexOperator.GTE);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Test]
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
			Assert.AreEqual(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.AreEqual(firstExpression.Op, IndexOperator.LT);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}

		[Test]
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
			Assert.AreEqual(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Column_name, typeof(BytesType)), (BytesType)columnName);
			Assert.AreEqual(firstExpression.Op, IndexOperator.LTE);
			Assert.AreEqual(CassandraType.GetTypeFromDatabaseValue(firstExpression.Value, typeof(BytesType)), (BytesType)columnValue);
		}
	}
}