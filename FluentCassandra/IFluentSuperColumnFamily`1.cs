using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>
		:	IFluentBaseColumnFamily,
			IFluentRecord<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
	}
}
