using System;

namespace FluentCassandra.Configuration
{
	public interface IKeySetMapping<T> : IKeyMapping, IHideObjectMembers
	{
	}
}
