using System;
using System.Dynamic;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentRecordExpression : IDynamicMetaObjectProvider
	{
		CassandraObject this[CassandraObject name] { get; }
	}
}