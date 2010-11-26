using System;

namespace FluentCassandra.Configuration
{
	/// <summary>
	/// 
	/// </summary>
	/// <remarks>Borrowed much of the layout from NoRM, I just couldn't resist it was very elegant in its design.</remarks>
	/// <see href="http://github.com/robconery/NoRM/tree/master/NoRM/Configuration/"/>
	public static class CassandraConfiguration
	{
		internal static event Action<Type> TypeConfigurationChanged;

		private static readonly object _lock = new object();
		private static IConfigurationContainer _configuration;

		/// <summary>
		/// 
		/// </summary>
		internal static IConfigurationContainer ConfigurationContainer
		{
			get
			{
				if (_configuration == null)
				{
					lock (_lock)
					{
						if (_configuration == null)
						{
							_configuration = new CassandraConfigurationContainer();
						}
					}
				}

				return _configuration;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		public static ITypeGetConfiguration<T> GetMapFor<T>()
		{
			if (_configuration != null)
				return _configuration.GetFor<T>();

			return null;
		}

		/// <summary>
		/// Kill a map for the specified type.
		/// </summary>
		/// <remarks>This is here for unit testing support, use at your own risk.</remarks>
		/// <typeparam name="T"></typeparam>
		public static void RemoveMapFor<T>()
		{
			if (_configuration != null)
				_configuration.RemoveFor<T>();
		}

		/// <summary>
		/// Allows various objects to fire type change event.
		/// </summary>
		/// <param name="t"></param>
		internal static void FireTypeChangedEvent(Type t)
		{
			if (TypeConfigurationChanged != null)
				CassandraConfiguration.TypeConfigurationChanged(t);
		}

		/// <summary>
		/// Given this singleton IConfigurationContainer, add a fluently-defined map.
		/// </summary>
		/// <param name="action">The action.</param>
		public static void Initialize(Action<IConfigurationContainer> action)
		{
			action(ConfigurationContainer);
		}
	}
}
