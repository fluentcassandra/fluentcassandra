using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	internal static class CassandraSlicePredicateBuilder
	{
		public static QueryableColumnFamilyOperation<TResult> BuildQueryableOperation<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source)
			where CompareWith : CassandraObject
		{
			return BuildOperation<QueryableColumnFamilyOperation<TResult>>(source, source.Setup.CreateQueryOperation);
		}

		public static ColumnFamilyOperation<TResult> BuildOperation<TResult>(this ICassandraQueryable source, Func<CassandraQuerySetup, CassandraSlicePredicate, ColumnFamilyOperation<TResult>> createOp)
		{
			return BuildOperation<ColumnFamilyOperation<TResult>>(source, createOp);
		}

		private static TOperation BuildOperation<TOperation>(this ICassandraQueryable source, Func<CassandraQuerySetup, CassandraSlicePredicate, TOperation> createOp)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			var calls = BuildCallDictionary(new Dictionary<string, object>(), source.Expression);

			var predicate = BuildPredicateFromExpression(source.Setup, calls);
			var operation = createOp(source.Setup, predicate);

			return operation;
		}

		private static CassandraSlicePredicate BuildPredicateFromExpression(CassandraQuerySetup setup, IDictionary<string, object> calls)
		{
			object fetch, take, takeUntil, superColumnName;

			if (!calls.TryGetValue("Fetch", out fetch))
				fetch = new CassandraObject[0];

			var columns = (CassandraObject[])fetch;
			CassandraRangeSlicePredicate predicate;

			if (columns.Length > 1)
			{
				if (calls.Count > 1)
					throw new CassandraException("A multi column fetch cannot be used with the following query options: " + String.Join(", ", calls.Keys.Where(x => x != "Fetch")));

				return new CassandraColumnSlicePredicate(columns);
			}
			else if (columns.Length == 1)
			{
				if (calls.Count == 1)
					return new CassandraColumnSlicePredicate(columns);

				predicate = new CassandraRangeSlicePredicate(columns[0], null);
			}
			else
			{
				predicate = new CassandraRangeSlicePredicate(null, null);
			}

			if (calls.TryGetValue("Take", out take))
			{
				int count = (int)take;
				predicate.Count = count;
			}

			if (calls.TryGetValue("TakeUntil", out takeUntil))
			{
				CassandraObject column = (CassandraObject)takeUntil;
				predicate.Finish = column;
			}

			if (calls.ContainsKey("Reverse"))
				predicate.Reversed = true;

			if (calls.TryGetValue("ForSuperColumn", out superColumnName))
				setup.SuperColumnName = (CassandraObject)superColumnName;

			return predicate;
		}

		private static IDictionary<string, object> BuildCallDictionary(IDictionary<string, object> calls, Expression exp)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.Call:
					return VisitMethodCall(calls, (MethodCallExpression)exp);

				case ExpressionType.Constant:
					return calls;

				default:
					throw new NotSupportedException(exp.NodeType + " is not a supported expression.");
			}
		}

		private static IDictionary<string, object> VisitMethodCall(IDictionary<string, object> calls, MethodCallExpression exp)
		{
			calls = BuildCallDictionary(calls, exp.Arguments[0]);

			switch (exp.Method.Name)
			{
				case "Fetch":
				case "Take":
				case "TakeUntil":
				case "ForSuperColumn":
					calls.Add(exp.Method.Name, ((ConstantExpression)exp.Arguments[1]).Value);
					break;

				case "Reverse":
					calls.Add(exp.Method.Name, null);
					break;

				default:
					throw new NotSupportedException(String.Format("Method call to {0} is not supported.", exp.Method.Name));
			}

			return calls;
		}
	}
}