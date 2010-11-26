using System;
using System.Collections.Generic;

namespace FluentCassandra.Configuration
{
	public interface ITypeGetConfiguration<T> : ITypeConfiguration
	{
		/// <summary>
		/// 
		/// </summary>
		IKeyGetMapping<T> KeyMap { get; }

		/// <summary>
		/// Gets the property maps.
		/// </summary>
		/// <value>The property maps.</value>
		IList<IPropertyGetMapping<T>> PropertyMap { get; }

		/// <summary>
		/// 
		/// </summary>
		string ColumnFamily { get; }

		/// <summary>
		/// 
		/// </summary>
		ColumnType ColumnType { get; }
	}
}
