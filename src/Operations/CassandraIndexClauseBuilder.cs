using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Cassandra = Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	internal static class CassandraIndexClauseBuilder
	{
		public static List<Apache.Cassandra.IndexExpression> Evaluate(Expression exp, List<Apache.Cassandra.IndexExpression> expressions = null)
		{
			if (expressions == null)
				expressions = new List<Cassandra.IndexExpression>();

			switch (exp.NodeType)
			{
				case ExpressionType.Lambda:
					return Evaluate(((LambdaExpression)exp).Body, expressions);

				case ExpressionType.And:
				case ExpressionType.AndAlso:
				case ExpressionType.Or:
				case ExpressionType.OrElse:
					return VisitConditionalExpression((BinaryExpression)exp, expressions);

				default:
					expressions.AddIndexExpression(exp);
					break;
			}

			return expressions;
		}

		private static List<Apache.Cassandra.IndexExpression> VisitConditionalExpression(BinaryExpression exp, List<Apache.Cassandra.IndexExpression> expressions)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.And:
				case ExpressionType.AndAlso:
					expressions = Evaluate(exp.Left, expressions);
					expressions = Evaluate(exp.Right, expressions);
					break;

				default:
					throw new NotSupportedException(exp.NodeType.ToString() + " is not a supported conditional criteria.");
			}

			return expressions;
		}

		public static void AddIndexExpression(this List<Apache.Cassandra.IndexExpression> expressions, Expression exp)
		{
			var indexExpression = VisitExpression(exp);

			if (indexExpression != null)
				expressions.Add(indexExpression);
		}

		private static Expression SimplifyExpression(Expression exp, ExpressionType? returnType = null)
		{
			if (returnType.HasValue && returnType.Value == exp.NodeType)
				return exp;

			switch (exp.NodeType)
			{
				case ExpressionType.Convert:
				case ExpressionType.Quote:
					return SimplifyExpression(((UnaryExpression)exp).Operand, returnType);

				case ExpressionType.Lambda:
					return SimplifyExpression(((LambdaExpression)exp).Body, returnType);

				default:
					return exp;
			}
		}

		private static CassandraObject GetColumnName(Expression exp)
		{
			exp = SimplifyExpression(exp);

			if (exp.NodeType != ExpressionType.Call && ((MethodCallExpression)exp).Method.Name == "get_Item")
				throw new NotSupportedException(exp.NodeType.ToString() + " is not supported, the left side of the expression must be a column name reference and look something like \"family[columnName]\".");

			var mExp = (MethodCallExpression)exp;
			var columnName = Expression.Lambda(mExp.Arguments[0]).Compile().DynamicInvoke();

			if (columnName is CassandraObject)
				return (CassandraObject)columnName;

			throw new CassandraException("The column name must be a CassandraType.");
		}

		private static Cassandra.IndexExpression VisitExpression(Expression exp)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.Convert:
				case ExpressionType.Lambda:
				case ExpressionType.Quote:
					return VisitExpression(SimplifyExpression(exp));

				case ExpressionType.Equal:
				case ExpressionType.GreaterThan:
				case ExpressionType.GreaterThanOrEqual:
				case ExpressionType.LessThan:
				case ExpressionType.LessThanOrEqual:
					return VisitRelationalExpression((BinaryExpression)exp);

				default:
					throw new NotSupportedException(exp.NodeType.ToString() + " is not supported.");
			}
		}

		private static Cassandra.IndexExpression VisitRelationalExpression(BinaryExpression exp)
		{
			Cassandra.IndexExpression indexExpression;

			var columnName = GetColumnName(exp.Left);
			var value = CassandraObject.GetCassandraObjectFromObject(Expression.Lambda(exp.Right).Compile().DynamicInvoke(), CassandraType.BytesType);

			indexExpression = new Cassandra.IndexExpression {
				Column_name = columnName.ToBigEndian(),
				Value = value.ToBigEndian()
			};

			switch (exp.NodeType)
			{
				case ExpressionType.Equal: indexExpression.Op = Cassandra.IndexOperator.EQ; break;
				case ExpressionType.GreaterThan: indexExpression.Op = Cassandra.IndexOperator.GT; break;
				case ExpressionType.GreaterThanOrEqual: indexExpression.Op = Cassandra.IndexOperator.GTE; break;
				case ExpressionType.LessThan: indexExpression.Op = Cassandra.IndexOperator.LT; break;
				case ExpressionType.LessThanOrEqual: indexExpression.Op = Cassandra.IndexOperator.LTE; break;

				default:
					throw new NotSupportedException(exp.NodeType.ToString() + " is not a supported relational criteria.");
			}

			return indexExpression;
		}
	}
}