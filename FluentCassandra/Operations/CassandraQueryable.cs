using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using System.Linq.Expressions;
using System.Reflection;

namespace FluentCassandra.Operations
{
	public static class CassandraQueryable
	{
		public static ICassandraQueryable<TResult, CompareWith> AsQueryable<TResult, CompareWith>(this QueryableColumnFamilyOperation<TResult, CompareWith> op, BaseCassandraColumnFamily family)
			where CompareWith: CassandraType
		{
			return new CassandraSlicePredicateQuery<TResult, CompareWith>(op, family);
		}

		public static ICassandraQueryable<TResult, CompareWith> Fetch<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source, params CompareWith[] columns)
			where CompareWith : CassandraType
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.CreateQuery(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression, Expression.Constant(columns) }));
		}

		public static ICassandraQueryable<TResult, CompareWith> Fetch<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source, CompareWith column)
			where CompareWith : CassandraType
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.CreateQuery(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression, Expression.Constant(column) }));
		}

		public static ICassandraQueryable<TResult, CompareWith> TakeUntil<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source, CompareWith column)
			where CompareWith : CassandraType
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.CreateQuery(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression, Expression.Constant(column) }));
		}

		public static ICassandraQueryable<TResult, CompareWith> Take<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source, int count)
			where CompareWith : CassandraType
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.CreateQuery(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression, Expression.Constant(count) }));
		}

		public static ICassandraQueryable<TResult, CompareWith> Reverse<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source)
			where CompareWith : CassandraType
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.CreateQuery(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression }));
		}

		public static IEnumerable<TResult> Execute<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source)
			where CompareWith : CassandraType
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Execute(source.Expression);
		}
	}
}
