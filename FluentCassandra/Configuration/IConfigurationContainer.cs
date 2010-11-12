using System;

namespace FluentCassandra.Configuration
{
	public interface IConfigurationContainer : ICassandraConfigurationMap
	{
		/// <summary>
		/// Registers a mongo type map implicitly.
		/// </summary>
		/// <typeparam name="T">Type to configure
		/// </typeparam>
		void AddMap<T>() where T : ICassandraConfigurationMap, new();

		/// <summary>
		/// Gets the configuration map.
		/// </summary>
		/// <returns>
		/// </returns>
		ICassandraConfigurationMap GetConfigurationMap();
	}
}
