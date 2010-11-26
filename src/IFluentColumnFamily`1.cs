using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentColumnFamily<CompareWith>
		:	IFluentBaseColumnFamily, 
			IFluentRecordHasFluentColumns<CompareWith>
		where CompareWith : CassandraType
	{
	}
}
