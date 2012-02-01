using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentRecordHasFluentColumns<CompareWith>
		:	IFluentRecord<IFluentColumn<CompareWith>>
		where CompareWith : CassandraType
	{
		CassandraType this[CompareWith columnName] { get; }
	}
}