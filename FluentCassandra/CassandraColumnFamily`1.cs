using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;
using FluentCassandra.Configuration;

namespace FluentCassandra
{
	public class CassandraColumnFamily<T> : CassandraColumnFamily
	{
		private ITypeGetConfiguration<T> _config;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public CassandraColumnFamily(CassandraKeyspace keyspace, IConnection connection)
			: base(keyspace, connection, CassandraConfiguration.GetMapFor<T>().ColumnFamily)
		{
			_config = CassandraConfiguration.GetMapFor<T>();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="obj"></param>
		/// <returns></returns>
		public FluentColumnFamily PrepareColumnFamily(T obj)
		{
			FluentColumnFamily record = new FluentColumnFamily(
				_config.KeyMap.KeyAccessor(obj),
				_config.ColumnFamily
			);

			foreach (var col in _config.PropertyMap)
			{
				var fcol = new FluentColumn {
					Name = col.Alias,
				};
				fcol.SetValue(col.PropertyAccessor(obj));
				record.Columns.Add(fcol);
			}

			return record;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="obj"></param>
		public void Add(T obj)
		{
			// TODO: wireup insert
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="obj"></param>
		public void Remove(T obj)
		{
			// TODO: wireup remove
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="key"></param>
		/// <param name="exp"></param>
		/// <returns></returns>
		public TResult Get<TResult>(string key, Expression<Func<T, TResult>> exp)
		{
			var name = GetName(exp);
			var selection = _config.PropertyMap.FirstOrDefault(x => x.PropertyName == name);
			var col = base.GetColumn(key, /* superColumn */ null, selection.Alias);

			return col.GetValue<TResult>();
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