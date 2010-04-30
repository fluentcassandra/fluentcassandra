using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;
using FluentCassandra.Configuration;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnFamily<CompareWith> : BaseCassandraColumnFamily
		where CompareWith : CassandraType
	{
		public CassandraColumnFamily(CassandraContext context, CassandraKeyspace keyspace, IConnection connection, string columnFamily)
			: base(context, keyspace, connection, columnFamily) { }

		public Type CompareWithType { get { return typeof(CompareWith); } }
	}
}

		/*
		private ITypeGetConfiguration<T> _config;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public CassandraColumnFamily(CassandraContext context, CassandraKeyspace keyspace, IConnection connection)
			: base(context, keyspace, connection, CassandraConfiguration.GetMapFor<T>().ColumnFamily)
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
}*/