using System;
using System.Linq;
using System.Runtime.CompilerServices;

namespace FluentCassandra.Linq
{
	public static class CqlMapperQueryExtensions
	{
		public static Boolean IsAnonymousType(this Type type)
		{
			if (type == null) return false;

			var hasCompilerGeneratedAttribute = type.GetCustomAttributes(typeof(CompilerGeneratedAttribute), false).Count() > 0;
			var nameContainsAnonymousType = type.FullName.Contains("AnonymousType");
			var isAnonymousType = hasCompilerGeneratedAttribute && nameContainsAnonymousType;

			return isAnonymousType;
		}

		public static IQueryable AsTypelessQuery(this IQueryable queryable)
		{
			if (queryable is CqlMapperQuery)
				return new CqlMapperQuery(queryable.Expression, (CqlMapperQueryProvider)queryable.Provider);

			return queryable;
		}
	}
}