using FluentCassandra.Types;
using System.Dynamic;

namespace FluentCassandra
{
	public interface IFluentRecordExpression : IDynamicMetaObjectProvider
	{
		CassandraObject this[CassandraObject name] { get; }
	}
}