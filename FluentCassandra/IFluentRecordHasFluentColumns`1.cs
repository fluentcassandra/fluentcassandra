using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentRecordHasFluentColumns<CompareWith>
		:	IFluentRecord<IFluentColumn<CompareWith>>
		where CompareWith : CassandraType
	{
		BytesType this[CompareWith columnName] { get; }
	}
}