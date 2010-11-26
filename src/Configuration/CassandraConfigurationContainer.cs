using System;

namespace FluentCassandra.Configuration
{
	public class CassandraConfigurationContainer : CassandraConfigurationMap, IConfigurationContainer
	{
		/// <summary>
		/// Registers a Cassandra Configuration Map by calling the default 
		/// constructor of T (so that's where you should add your mapping logic)
		/// </summary>
		/// <remarks>
		/// BY CONVENTION, the default constructor of T should register the mappings that are relevant.
		/// </remarks>
		/// <typeparam name="T">
		/// The type of the map that should be added.
		/// </typeparam>
		public void AddMap<T>() where T : ICassandraConfigurationMap, new()
		{
			//this is semi-magical, look at remarks as to why this does anything.
			new T();
		}

		/// <summary>
		/// Gets the configuration map.
		/// </summary>
		/// <returns>
		/// </returns>
		public ICassandraConfigurationMap GetConfigurationMap()
		{
			return this;
		}
	}
}
