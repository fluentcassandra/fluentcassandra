using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
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
