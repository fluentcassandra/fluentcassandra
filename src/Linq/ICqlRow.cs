using System;
using System.Linq;
using FluentCassandra.Types;
using System.Dynamic;

namespace FluentCassandra.Linq
{
	public interface ICqlRow<CompareWith> : IDynamicMetaObjectProvider
		where CompareWith : CassandraType
	{
		BytesType Key { get; set; }
		BytesType this[CompareWith columnName] { get; }
	}
}
