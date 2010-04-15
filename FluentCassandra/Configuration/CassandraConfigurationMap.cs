using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Configuration
{
	public class CassandraConfigurationMap : ICassandraConfigurationMap
	{
		private static readonly Dictionary<Type, ITypeConfiguration> _configMap = new Dictionary<Type, ITypeConfiguration>();

		/// <summary>
		/// Configures properties for type T
		/// </summary>
		/// <typeparam name="T">Type to configure</typeparam>
		/// <param name="typeConfigurationAction">The type configuration action.</param>
		public void For<T>(Action<ITypeSetConfiguration<T>> typeConfigurationAction)
		{
			var typeConfig = new CassandraTypeConfiguration<T>();
			_configMap[typeof(T)] = typeConfig;
			typeConfigurationAction((ITypeSetConfiguration<T>)typeConfig);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		public ITypeGetConfiguration<T> GetFor<T>()
		{
			return (ITypeGetConfiguration<T>)_configMap[typeof(T)];
		}

		/// <summary>
		/// Removes the mapping for this type.
		/// </summary>
		/// <remarks>
		/// Added to support Unit testing. Use at your own risk!
		/// </remarks>
		/// <typeparam name="T"></typeparam>
		public void RemoveFor<T>()
		{
			_configMap.Remove(typeof(T));
		}
	}
}
