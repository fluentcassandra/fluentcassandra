using System;
using System.ComponentModel;
using System.Collections.Generic;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentColumnFamily<CompareWith>
		:	IFluentBaseColumnFamily, 
			IFluentRecord<IFluentColumn<CompareWith>>
		where CompareWith : CassandraType
	{
	}
}
