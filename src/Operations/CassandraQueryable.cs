using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;
using System.Linq.Expressions;
using System.Reflection;
using FluentCassandra.Operations;

namespace FluentCassandra
{
	public static class CassandraQueryable
	{
		public static ICassandraQueryable<TResult, CompareWith> Fetch<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source, params CompareWith[] columns)
			where CompareWith : CassandraObject
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Provider.CreateQuery(source.Setup, Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression, Expression.Constant(columns) }));
		}

		public static ICassandraQueryable<TResult, CompareWith> TakeUntil<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source, CompareWith column)
			where CompareWith : CassandraObject
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Provider.CreateQuery(source.Setup, Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression, Expression.Constant(column) }));
		}

		public static ICassandraQueryable<TResult, CompareWith> ForSuperColumn<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source, CassandraObject superColumnName)
			where CompareWith : CassandraObject
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Provider.CreateQuery(source.Setup, Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression, Expression.Constant(superColumnName) }));
		}

		public static ICassandraQueryable<TResult, CompareWith> Take<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source, int count)
			where CompareWith : CassandraObject
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Provider.CreateQuery(source.Setup, Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression, Expression.Constant(count) }));
		}

		public static ICassandraQueryable<TResult, CompareWith> Reverse<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source)
			where CompareWith : CassandraObject
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Provider.CreateQuery(source.Setup, Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression }));
		}

		public static int Count<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source)
			where CompareWith : CassandraObject
		{
			if (source == null)
				throw new ArgumentNullException("source");

			source = source.Provider.CreateQuery(source.Setup, Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression }));
			return source.Provider.Execute<int>(source, (x, slice) => new ColumnCount(x.Key, x.SuperColumnName, slice));
		}

		public static IDictionary<CassandraObject, int> MultiCount<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source)
			where CompareWith : CassandraObject
		{
			if (source == null)
				throw new ArgumentNullException("source");

			source = source.Provider.CreateQuery(source.Setup, Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TResult), typeof(CompareWith) }), new Expression[] { source.Expression }));
			return source.Provider.Execute<IDictionary<CassandraObject, int>>(source, (x, slice) => new MultiGetColumnCount(x.Keys, x.SuperColumnName, slice));
		}

		public static IEnumerable<TResult> Execute<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source)
			where CompareWith : CassandraObject
		{
			if (source == null)
				throw new ArgumentNullException("source");

			return source.Provider.ExecuteQuery(source);
		}

		public static IEnumerable<dynamic> ExecuteDynamic<TResult, CompareWith>(this ICassandraQueryable<TResult, CompareWith> source)
			where CompareWith : CassandraObject
		{
			return Execute(source).Cast<dynamic>();
		}

		public static dynamic AsDynamic(this IFluentRecord record)
		{
			return record;
		}
	}
}
