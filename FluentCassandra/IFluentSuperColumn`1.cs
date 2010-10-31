using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentSuperColumn<CompareWith, CompareSubcolumnWith>
		:	IFluentSuperColumn,
			IFluentBaseColumn<CompareWith>,
			IFluentRecordHasFluentColumns<CompareSubcolumnWith>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		new IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> Family { get; }
	}
}