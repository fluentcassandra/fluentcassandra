using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;
using System.Linq.Expressions;
using System.Reflection;
using FluentCassandra.Operations;

namespace FluentCassandra
{
	public static class CassandraSlicePredicateQueryable
	{
		public static CassandraSlicePredicateQuery<TResult> TakeKeys<TResult>(this CassandraSlicePredicateQuery<TResult> source, int count)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(count) }));
		}

		public static CassandraSlicePredicateQuery<TResult> TakeColumns<TResult>(this CassandraSlicePredicateQuery<TResult> source, int count)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(count) }));
		}

		public static CassandraSlicePredicateQuery<TResult> FetchKeys<TResult>(this CassandraSlicePredicateQuery<TResult> source, params CassandraObject[] keys)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(keys) }));
		}

		public static CassandraSlicePredicateQuery<TResult> FetchColumns<TResult>(this CassandraSlicePredicateQuery<TResult> source, params CassandraObject[] columns)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(columns) }));
		}

		public static CassandraSlicePredicateQuery<TResult> StartWithKey<TResult>(this CassandraSlicePredicateQuery<TResult> source, CassandraObject key)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(key) }));
		}

		public static CassandraSlicePredicateQuery<TResult> TakeUntilKey<TResult>(this CassandraSlicePredicateQuery<TResult> source, CassandraObject key)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(key) }));
		}

		public static CassandraSlicePredicateQuery<TResult> StartWithToken<TResult>(this CassandraSlicePredicateQuery<TResult> source, string token)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(token) }));
		}

		public static CassandraSlicePredicateQuery<TResult> TakeUntilToken<TResult>(this CassandraSlicePredicateQuery<TResult> source, string token)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(token) }));
		}

		public static CassandraSlicePredicateQuery<TResult> StartWithColumn<TResult>(this CassandraSlicePredicateQuery<TResult> source, CassandraObject column)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(column) }));
		}

		public static CassandraSlicePredicateQuery<TResult> TakeUntilColumn<TResult>(this CassandraSlicePredicateQuery<TResult> source, CassandraObject column)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(column) }));
		}

		public static CassandraSlicePredicateQuery<TResult> ForSuperColumn<TResult>(this CassandraSlicePredicateQuery<TResult> source, CassandraObject superColumnName)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(superColumnName) }));
		}

		public static CassandraSlicePredicateQuery<TResult> ReverseColumns<TResult>(this CassandraSlicePredicateQuery<TResult> source)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression }));
		}

		public static CassandraSlicePredicateQuery<TResult> Where<TResult>(this CassandraSlicePredicateQuery<TResult> source, Expression<Func<IFluentRecordExpression, bool>> expression)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression, Expression.Constant(expression) }));
		}

		public static int CountColumns<TResult>(this CassandraSlicePredicateQuery<TResult> source)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			source = source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression }));
			var op = (ColumnCount)source.BuildQueryableOperation();
			return source.Family.Context.ExecuteOperation(op);
		}

		public static IDictionary<CassandraObject, int> CountColumnsByKey<TResult>(this CassandraSlicePredicateQuery<TResult> source)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			source = source.Family.CreateCassandraSlicePredicateQuery<TResult>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult) }), new Expression[] { source.Expression }));
			var op = (MultiGetColumnCount)source.BuildQueryableOperation();
			return source.Family.Context.ExecuteOperation(op);
		}

		public static IEnumerable<TResult> Execute<TResult>(this CassandraSlicePredicateQuery<TResult> source)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Family.ExecuteCassandraSlicePredicateQuery(source);
		}

		public static IEnumerable<dynamic> ExecuteDynamic<TResult>(this CassandraSlicePredicateQuery<TResult> source)
		{
			return Execute(source).Cast<dynamic>();
		}

		public static dynamic AsDynamic(this IFluentRecord record)
		{
			return record;
		}
	}
}
