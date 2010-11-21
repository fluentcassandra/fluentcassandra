using System;
using System.Linq.Expressions;

namespace FluentCassandra.Configuration
{
	public interface ITypeSetConfiguration<T> : ITypeConfiguration
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		void UseColumnFamily(string columnFamily);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnType"></param>
		void UseColumnType(ColumnType columnType);

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
