using System;

namespace FluentCassandra.Configuration
{
	public interface IPropertySetMapping<T> : IPropertMapping, IHideObjectMembers
	{
		/// <summary>
		/// Uses the alias for a given type's property.
		/// </summary>
		/// <param name="alias">
		/// The alias.
		/// </param>
		void UseAlias(string alias);
	}
}
