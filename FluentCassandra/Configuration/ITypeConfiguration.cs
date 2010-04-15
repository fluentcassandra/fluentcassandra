using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace FluentCassandra.Configuration
{
	public interface ITypeConfiguration
	{
	}

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
	}

	public interface ITypeSetConfiguration<T> : ITypeConfiguration
	{
		void UseColumnFamily(string columnFamily);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="exp"></param>
		/// <returns></returns>
		IKeySetMapping<T> ForKey(Expression<Func<T, string>> exp);

		/// <summary>
		/// Looks up property names for use with aliases.
		/// </summary>
		/// <param name="exp">The source propery.</param>
		/// <returns></returns>
		IPropertySetMapping<T> ForProperty(Expression<Func<T, object>> exp);
	}
}
