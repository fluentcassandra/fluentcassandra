using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using FluentCassandra.Types;
using System.Collections;

namespace FluentCassandra.Operations
{
	internal static class CassandraSlicePredicateBuilder
	{
		public static void BuildPredicate<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source)
			where CompareWith : CassandraType
		{
			if (source == null)
				throw new ArgumentNullException("source");

			var predicate = BuildPredicateFromExpression(source.Expression);
			source.Operation.SlicePredicate = predicate;
		}

		private static CassandraSlicePredicate BuildPredicateFromExpression(Expression exp)
		{
			var calls = BuildCallDictionary(new Dictionary<string, object>(), exp);

			object fetch, take, takeUntil;

			if (!calls.TryGetValue("Fetch", out fetch))
				throw new MissingMethodException("Fetch is a required call.", "Fetch");

			var columns = (CassandraType[])fetch;
			RangeSlicePredicate predicate;

			if (columns.Length > 1)
			{
				if (calls.Count > 1)
					throw new CassandraException("A multi column fetch cannot be used with the following query options: " + String.Join(", ", calls.Keys.Where(x => x != "Fetch")));

				return new ColumnSlicePredicate(columns);
			}
			else if (columns.Length == 1)
			{
				if (calls.Count == 1)
					return new ColumnSlicePredicate(columns);

				predicate = new RangeSlicePredicate(columns[0], null);
			}
			else
			{
				predicate = new RangeSlicePredicate(null, null);
			}

			if (calls.TryGetValue("Take", out take))
			{
				int count = (int)take;
				predicate.Count = count;
			}

			if (calls.TryGetValue("TakeUntil", out takeUntil))
			{
				CassandraType column = (CassandraType)takeUntil;
				predicate.Finish = column;
			}

			if (calls.ContainsKey("Reverse"))
				predicate.Reversed = true;

			return predicate;
		}

		private static IDictionary<string, object> BuildCallDictionary(IDictionary<string, object> calls, Expression exp)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.Call:
					return VisitMethodCall(calls, (MethodCallExpression)exp);

				default:
					throw new NotSupportedException(exp.NodeType + " is not a supported expression.");
			}
		}

		private static IDictionary<string, object> VisitMethodCall(IDictionary<string, object> calls, MethodCallExpression exp)
		{
			switch (exp.Method.Name)
			{
				case "Fetch":
				case "Take":
				case "TakeUntil":
				case "Reverse":
					calls.Add(exp.Method.Name, ((ConstantExpression)exp.Arguments[1]).Value);
					break;

				default:
					throw new NotSupportedException("Method call to " + exp.Method.Name + " is not supported.");
			}

			return calls;
		}
	}
}