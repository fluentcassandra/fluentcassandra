using System;
using System.Dynamic;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentRecordExpression : IDynamicMetaObjectProvider
	{
		CassandraType this[CassandraType name] { get; }
	}
}