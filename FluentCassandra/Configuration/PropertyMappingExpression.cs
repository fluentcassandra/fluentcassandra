using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Configuration
{
	public class PropertyMappingExpression<T> : IPropertyGetMapping<T>, IPropertySetMapping<T>
	{
		/// <summary>
		/// Gets or sets the alias.
		/// </summary>
		/// <value>The alias.</value>
		public string Alias { get; set; }

		/// <summary>
		/// Gets or sets the name of the source property.
		/// </summary>
		/// <value>The name of the source property.</value>
		public string PropertyName { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public Func<T, object> PropertyAccessor { get; set; }

		/// <summary>
		/// Uses the alias for a given type's property.
		/// </summary>
		/// <param name="alias">
		/// The alias.
		/// </param>
		public void UseAlias(string alias)
		{
			Alias = alias;
		}
	}
}
