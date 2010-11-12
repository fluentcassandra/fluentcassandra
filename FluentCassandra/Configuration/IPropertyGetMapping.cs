using System;

namespace FluentCassandra.Configuration
{
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
}
