using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace FluentCassandra.Configuration
{
	public class CassandraTypeConfiguration<T> : ITypeSetConfiguration<T>, ITypeGetConfiguration<T>
	{
		private IKeyMapping _key;
		private readonly IDictionary<string, IPropertMapping> _properties = new Dictionary<string, IPropertMapping>();

		/// <summary>
		/// 
		/// </summary>
		public IKeyGetMapping<T> KeyMap
		{
			get { return (IKeyGetMapping<T>)_key; }
		}

		/// <summary>
		/// Gets the property maps.
		/// </summary>
		/// <value>The property maps.</value>
		public IList<IPropertyGetMapping<T>> PropertyMap
		{
			get { return _properties.Values.Cast<IPropertyGetMapping<T>>().ToList(); }
		}

		/// <summary>
		/// 
		/// </summary>
		public string ColumnFamily
		{
			get;
			private set;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		public void UseColumnFamily(string columnFamily)
		{
			ColumnFamily = columnFamily;
			CassandraConfiguration.FireTypeChangedEvent(typeof(T));
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="exp"></param>
		/// <returns></returns>
		public IKeySetMapping<T> ForKey(Expression<Func<T, string>> exp)
		{
			var func = exp.Compile();
			var name = GetName(exp);
			_key = new KeyMappingExpression<T> {
				KeyAccessor = func,
				KeyName = name
			};

			CassandraConfiguration.FireTypeChangedEvent(typeof(T));
			return (IKeySetMapping<T>)_key;
		}

		/// <summary>
		/// Looks up property names for use with aliases.
		/// </summary>
		/// <param name="sourcePropery">The source propery.</param>
		/// <returns></returns>
		public IPropertySetMapping<T> ForProperty(Expression<Func<T, object>> exp)
		{
			var func = exp.Compile();
			var name = GetName(exp);
			var mapExp = new PropertyMappingExpression<T> {
				PropertyAccessor = func,
				PropertyName = name,
				Alias = name	// this will get changed if the user calls UseAlias
			};
			_properties[name] = mapExp;

			CassandraConfiguration.FireTypeChangedEvent(typeof(T));
			return mapExp;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="exp"></param>
		/// <returns></returns>
		private string GetName(Expression exp)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.MemberAccess:
					return ((MemberExpression)exp).Member.Name;

				case ExpressionType.Convert:
				case ExpressionType.Quote:
					return GetName(((UnaryExpression)exp).Operand);

				case ExpressionType.Lambda:
					return GetName(((LambdaExpression)exp).Body);

				default:
					return null;
			}
		}
	}
}
