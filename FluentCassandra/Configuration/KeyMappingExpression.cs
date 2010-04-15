using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Configuration
{
	public class KeyMappingExpression<T> : IKeyGetMapping<T>, IKeySetMapping<T>
	{
		/// <summary>
		/// Gets or sets the name of the source property.
		/// </summary>
		/// <value>The name of the source property.</value>
		public string KeyName { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public Func<T, string> KeyAccessor { get; set; }
	}
}
