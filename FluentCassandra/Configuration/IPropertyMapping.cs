using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Configuration
{
	public interface IPropertMapping
	{
	}

	public interface IPropertyGetMapping<T> : IPropertMapping, IHideObjectMembers
	{
		string Alias { get; }

		/// <summary>
		/// Gets or sets the name of the source property.
		/// </summary>
		/// <value>The name of the source property.</value>
		string PropertyName { get; }

		/// <summary>
		/// 
		/// </summary>
		Func<T, object> PropertyAccessor { get; }
	}

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
