using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentSuperColumn<CompareWith, CompareSubcolumnWith>
		:	IFluentSuperColumn,
			IFluentBaseColumn<CompareWith>,
			IFluentRecord<IFluentColumn<CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		new IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> Family { get; }
	}
}