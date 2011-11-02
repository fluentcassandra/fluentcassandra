using System;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Linq
{
	public interface ICqlRow<CompareWith>
		where CompareWith : CassandraType
	{
		BytesType Key { get; set; }
		BytesType this[CompareWith columnName] { get; }
	}
}
