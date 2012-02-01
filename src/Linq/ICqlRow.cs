using System;
using System.Linq;
using FluentCassandra.Types;
using System.Dynamic;
using System.Collections.Generic;

namespace FluentCassandra.Linq
{
	public interface ICqlRow<CompareWith> : IDynamicMetaObjectProvider
		where CompareWith : CassandraType
	{
		CassandraType Key { get; set; }
		CassandraType this[CompareWith columnName] { get; }

		IList<IFluentColumn<CompareWith>> Columns { get; }
	}
}
