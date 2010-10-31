using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations.Test
{
	[TestClass]
	public class CassandraIndexClauseTest
	{
		[TestMethod]
		public void StartKeySet()
		{
			// arrange
			string key = "test";
			int count = 20;
			string columnName = "test-column";
			string columnValue = "test-value";

			// act
			var index = new CassandraIndexClause<BytesType>(
				key,
				count,
				family => family[columnName] == columnValue);

			// assert
			Assert.AreEqual(index.StartKey, key);
		}

		[TestMethod]
		public void CountSet()
		{
			// arrange
			string key = "test";
			int count = 20;
			string columnName = "test-column";
			string columnValue = "test-value";

			// act
			var index = new CassandraIndexClause<BytesType>(
				key,
				count,
				family => family[columnName] == columnValue);

			// assert
			Assert.AreEqual(index.Count, count);
		}

		[TestMethod]
		public void SingleExpression()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName = "test-column";
			var columnValue = "test-value";

			// act
			var index = new CassandraIndexClause<BytesType>(
				key,
				count,
				family => family[columnName] == columnValue);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.AreEqual(1, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual((BytesType)firstExpression.Column_name, (BytesType)columnName);
			Assert.AreEqual(firstExpression.Op, IndexOperator.EQ);
			Assert.AreEqual((BytesType)firstExpression.Value, (BytesType)columnValue);
		}

		[TestMethod]
		public void MultiExpression()
		{
			// arrange
			string key = "test";
			int count = 20;
			var columnName1 = "test1-column";
			var columnValue1 = "test1-value";
			var columnName2 = "test2-column";
			var columnValue2 = Math.PI;

			// act
			var index = new CassandraIndexClause<BytesType>(
				key,
				count,
				family => family[columnName1] == columnValue1
					&& family[columnName2] > columnValue2);
			var expressions = index.CompiledExpressions;

			// assert
			Assert.AreEqual(2, expressions.Count);

			var firstExpression = expressions[0];
			Assert.IsNotNull(firstExpression);
			Assert.AreEqual((BytesType)firstExpression.Column_name, (BytesType)columnName1);
			Assert.AreEqual(firstExpression.Op, IndexOperator.EQ);
			Assert.AreEqual((BytesType)firstExpression.Value, (BytesType)columnValue1);

			var secondExpression = expressions[1];
			Assert.AreEqual((BytesType)secondExpression.Column_name, (BytesType)columnName2);
			Assert.AreEqual(secondExpression.Op, IndexOperator.GT);
			Assert.AreEqual((BytesType)secondExpression.Value, (BytesType)columnValue2);
		}
	}
}
