﻿using FluentCassandra.Types;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace FluentCassandra.Linq
{
	public static class CqlQueryExtensions
	{
		public static CqlQuery Select(this IQueryable<ICqlRow> source, params CassandraObject[] columns)
		{
			if (source == null)
				throw new ArgumentNullException("source");

			if (columns == null)
				throw new ArgumentNullException("columns");

			return (CqlQuery)source.Provider.CreateQuery<ICqlRow>(Expression.Call(null, (MethodInfo)MethodBase.GetCurrentMethod(), new Expression[] { source.Expression, Expression.Constant(columns) }));
		}
	}
}