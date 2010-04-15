using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Configuration
{
	public interface IKeyMapping
	{
	}

	public interface IKeyGetMapping<T> : IKeyMapping, IHideObjectMembers
	{
		/// <summary>
		/// Gets or sets the name of the source property.
		/// </summary>
		/// <value>The name of the source property.</value>
		string KeyName { get; set; }

		/// <summary>
		/// 
		/// </summary>
		Func<T, string> KeyAccessor { get; set; }
	}

	public interface IKeySetMapping<T> : IKeyMapping, IHideObjectMembers
	{
	}
}
