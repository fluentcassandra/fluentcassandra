using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentRecordExpression
	{
		CassandraType this[CassandraType name] { get; }
	}
}