using System;
using System.Linq;

namespace FluentCassandra.Linq
{
	public interface ICassandraColumnFamilyInfo
	{
		string FamilyName { get; }
	}
}
