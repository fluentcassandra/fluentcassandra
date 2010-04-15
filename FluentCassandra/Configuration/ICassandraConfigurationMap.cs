using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Configuration
{
	public interface ICassandraConfigurationMap
	{
		/// <summary>
		/// Fluently define a configuration for the specified type. This will be merged with any existing types.
		/// </summary>
		/// <typeparam name="T">Object type under property mapping</typeparam>
		/// <param name="typeConfiguration">The type configuration.</param>
		void For<T>(Action<ITypeSetConfiguration<T>> typeConfiguration);


		/// <summary>
		/// Remove all configuration for the specified type.
		/// </summary>
		/// <remarks>Supports unit testing, use at your own risk!</remarks>
		/// <typeparam name="T">The type for which to remove fluent mappings.</typeparam>
		void RemoveFor<T>();

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		ITypeGetConfiguration<T> GetFor<T>();
	}
}
