using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	internal static class CassandraSlicePredicateBuilder
	{
		public static object BuildQueryableOperation<TResult>(this CassandraSlicePredicateQuery<TResult> source)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			var calls = BuildCallDictionary(new Dictionary<string, object>(), source.Expression);
			
			DoPreCheck(calls);

			var setup = BuildQuerySetup(calls);
			var predicate = BuildSlicePredicate(setup);
			var operation = BuildOperation<TResult>(setup, predicate, calls);

			return operation;
		}

		private static void DoPreCheck(IDictionary<string, object> calls)
		{
			if (calls.ContainsKey("CountColumnsByKey") && !calls.ContainsKey("FetchKeys"))
				throw new CassandraException("'CountColumnsByKey' must be used with the following query option: 'FetchKeys'");

			if (calls.ContainsKey("CountColumns") && !calls.ContainsKey("FetchKeys"))
				throw new CassandraException("'CountColumns' must be used with the following query option: 'FetchKeys'");

			if (calls.ContainsKey("FetchKeys") && (calls.ContainsKey("StartWithKey") || calls.ContainsKey("TakeUntilKey") || calls.ContainsKey("StartWithToken") || calls.ContainsKey("TakeUntilToken") || calls.ContainsKey("TakeKeys")))
				throw new CassandraException("'FetchKeys' cannot be used with the following query options: 'TakeKeys', 'StartWithKey', 'TakeUntilKey', 'StartWithToken', 'TakeUntilToken'");

			if (calls.ContainsKey("FetchColumns") && (calls.ContainsKey("StartWithColumn") || calls.ContainsKey("TakeUntilColumn") || calls.ContainsKey("ReverseColumns") || calls.ContainsKey("TakeColumns")))
				throw new CassandraException("'FetchColumns' cannot be used with the following query options: 'TakeColumns', 'StartWithColumn', 'TakeUntilColumn', 'ReverseColumns'");
		}

		private static CassandraQuerySetup BuildQuerySetup(IDictionary<string, object> calls)
		{
			var setup = new CassandraQuerySetup();

			object keyCount;
			if (calls.TryGetValue("TakeKeys", out keyCount))
				setup.KeyCount = (int)keyCount;

			object columnCount;
			if (calls.TryGetValue("TakeColumns", out columnCount))
				setup.ColumnCount = (int)columnCount;

			object keys;
			if (calls.TryGetValue("FetchKeys", out keys))
				setup.Keys = (CassandraObject[])keys;

			object columns;
			if (calls.TryGetValue("FetchColumns", out columns))
				setup.Columns = (CassandraObject[])columns;

			object startKey;
			if (calls.TryGetValue("StartWithKey", out startKey))
				setup.StartKey = (CassandraObject)startKey;

			object endKey;
			if (calls.TryGetValue("TakeUntilKey", out endKey))
				setup.EndKey = (CassandraObject)endKey;

			object startColumn;
			if (calls.TryGetValue("StartWithColumn", out startColumn))
				setup.StartColumn = (CassandraObject)startColumn;

			object endColumn;
			if (calls.TryGetValue("TakeUntilColumn", out endColumn))
				setup.EndColumn = (CassandraObject)endColumn;

			object startToken;
			if (calls.TryGetValue("StartWithToken", out startToken))
				setup.StartToken = (string)startToken;

			object endToken;
			if (calls.TryGetValue("TakeUntilToken", out endToken))
				setup.EndToken = (string)endToken;

			object superColumnName;
			if (calls.TryGetValue("ForSuperColumn", out superColumnName))
				setup.SuperColumnName = (CassandraObject)superColumnName;

			object indexClause;
			if (calls.TryGetValue("Where", out indexClause))
				setup.IndexClause = (Expression<Func<IFluentRecordExpression, bool>>)indexClause;

			setup.Reverse = calls.ContainsKey("ReverseColumns");

			if (calls.ContainsKey("CountColumnsByKey") && setup.Columns.Count() > 0)
				throw new CassandraException("'FetchKeys' must contain at least one key when being used with 'CountColumnsByKey'");

			if (calls.ContainsKey("CountColumns") && setup.Columns.Count() > 1)
				throw new CassandraException("'FetchKeys' must contain only one key when being used with 'CountColumns'");

			return setup;
		}

		private static CassandraSlicePredicate BuildSlicePredicate(CassandraQuerySetup setup)
		{
			if (setup.Columns.Count() > 0)
				return new CassandraColumnSlicePredicate(setup.Columns);

			return new CassandraRangeSlicePredicate(
				setup.StartColumn,
				setup.EndColumn,
				setup.Reverse,
				setup.ColumnCount);
		}

		private static object BuildOperation<TResult>(CassandraQuerySetup setup, CassandraSlicePredicate predicate, IDictionary<string, object> calls)
		{
			if (calls.ContainsKey("CountColumnsByKey"))
				return new MultiGetColumnCount(setup.Keys, setup.SuperColumnName, predicate);

			if (calls.ContainsKey("CountColumns"))
				return new ColumnCount(setup.Keys.First(), setup.SuperColumnName, predicate);

			var forSuperColumn = typeof(TResult) == typeof(FluentSuperColumnFamily);

			if (!forSuperColumn && setup.SuperColumnName != null)
				throw new CassandraException("'ForSuperColumn' can only be used on a super column family.");

			if (forSuperColumn)
			{
				if (setup.IndexClause != null)
				{
					var indexClause = new CassandraIndexClause(setup.StartKey, setup.ColumnCount, setup.IndexClause);
					return new GetSuperColumnFamilyIndexedSlices(indexClause, predicate);
				}
				else if (setup.Keys.Count() > 0)
				{
					return new MultiGetSuperColumnFamilySlice(setup.Keys, setup.SuperColumnName, predicate);
				}
				else
				{
					var keyRange = new CassandraKeyRange(setup.StartKey, setup.EndKey, setup.StartToken, setup.EndToken, setup.KeyCount);
					return new GetSuperColumnFamilyRangeSlices(keyRange, setup.SuperColumnName, predicate);
				}
			}
			else
			{
				if (setup.IndexClause != null)
				{
					var indexClause = new CassandraIndexClause(setup.StartKey, setup.KeyCount, setup.IndexClause);
					return new GetColumnFamilyIndexedSlices(indexClause, predicate);
				}
				else if (setup.Keys.Count() > 0)
				{
					return new MultiGetColumnFamilySlice(setup.Keys, predicate);
				}
				else
				{
					var keyRange = new CassandraKeyRange(setup.StartKey, setup.EndKey, setup.StartToken, setup.EndToken, setup.KeyCount);
					return new GetColumnFamilyRangeSlices(keyRange, predicate);
				}
			}
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
				case "TakeKeys":
				case "TakeColumns":
				case "FetchKeys":
				case "FetchColumns":
				case "StartWithKey":
				case "TakeUntilKey":
				case "StartWithToken":
				case "TakeUntilToken":
				case "StartWithColumn":
				case "TakeUntilColumn":
				case "ForSuperColumn":
				case "Where":
					calls.Add(exp.Method.Name, ((ConstantExpression)exp.Arguments[1]).Value);
					break;

				case "ReverseColumns":
				case "CountColumns":
				case "CountColumnsByKey":
					calls.Add(exp.Method.Name, null);
					break;

				default:
					throw new NotSupportedException(String.Format("Method call to {0} is not supported.", exp.Method.Name));
			}

			return calls;
		}
	}
}