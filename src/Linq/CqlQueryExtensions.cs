using System;
using System.Linq;
using FluentCassandra.Types;
using System.Linq.Expressions;
using System.Reflection;

namespace FluentCassandra.Linq
{
	public static class CqlQueryExtensions
	{
		public static CqlQuery<CompareWith> Select<CompareWith>(this IQueryable<ICqlRow<CompareWith>> source, params CompareWith[] columns) where CompareWith : CassandraType
		{
			if (source == null)
				throw new ArgumentNullException("source");

			if (columns == null)
				throw new ArgumentNullException("columns");

			return (CqlQuery<CompareWith>)source.Provider.CreateQuery<ICqlRow<CompareWith>>(Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(CompareWith) }), new Expression[] { source.Expression, Expression.Constant(columns) }));
		}
	}
}