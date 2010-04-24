using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Configuration;
using System.Collections;

namespace FluentCassandra
{
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public class CassandraColumnFamily
	{
		private CassandraKeyspace _keyspace;
		private IConnection _connection;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public CassandraColumnFamily(CassandraKeyspace keyspace, IConnection connection, string columnFamily)
		{
			_keyspace = keyspace;
			_connection = connection;
			FamilyName = columnFamily;
		}

		/// <summary>
		/// The keyspace the column family belongs to.
		/// </summary>
		public CassandraKeyspace Keyspace { get { return _keyspace; } }

		/// <summary>
		/// The family name for this column family.
		/// </summary>
		public string FamilyName { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		protected Cassandra.Client GetClient()
		{
			return _connection.Client;
		}

		/// <summary>
		/// Verifies that the family passed in is part of this family.
		/// </summary>
		/// <param name="family"></param>
		/// <returns></returns>
		private bool IsPartOfFamily(IFluentColumnFamily family)
		{
			return String.Equals(family.FamilyName, FamilyName);
		}

		/*
		 * i32 get_count(keyspace, key, column_parent, consistency_level) 
		 */

		/// <summary>
		/// 
		/// </summary>
		/// <param name="superColumn"></param>
		/// <returns></returns>
		public int ColumnCount(FluentSuperColumn superColumn)
		{
			return ColumnCount(superColumn.GetParent());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnParent"></param>
		/// <returns></returns>
		public int ColumnCount(FluentColumnParent columnParent)
		{
			return ColumnCount(
				columnParent.ColumnFamily,
				columnParent.SuperColumn == null ? null : columnParent.SuperColumn.Name
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		/// <param name="superColumnName"></param>
		/// <returns></returns>
		public int ColumnCount(IFluentColumnFamily record, string superColumnName = null)
		{
			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			return ColumnCount(record.Key, superColumnName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <returns></returns>
		public int ColumnCount(string key, string superColumnName)
		{
			var parent = new ColumnParent {
				Column_family = FamilyName
			};

			if (!String.IsNullOrEmpty(superColumnName))
				parent.Super_column = superColumnName.GetBytes();

			return GetClient().get_count(
				_keyspace.KeyspaceName,
				key,
				parent,
				ConsistencyLevel.ONE
			);
		}

		/*
		 * insert(keyspace, key, column_path, value, timestamp, consistency_level)
		 */

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		public void Insert(IFluentColumn col)
		{
			Insert(col.GetPath());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="path"></param>
		public void Insert(FluentColumnPath path)
		{
			if (IsPartOfFamily(path.ColumnFamily))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			Insert(
				path.ColumnFamily.Key,
				path.SuperColumn == null ? null : path.SuperColumn.Name,
				path.Column
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="col"></param>
		public void Insert(string key, string superColumnName, FluentColumn col)
		{
			Insert(
				key,
				superColumnName,
				col.Name,
				col.GetValue(),
				col.Timestamp
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="col"></param>
		public void Insert(string key, string superColumnName, string columnName, object value, DateTimeOffset timestamp)
		{
			var path = new ColumnPath {
				Column_family = FamilyName,
				Column = columnName.GetBytes()
			};

			if (!String.IsNullOrWhiteSpace(superColumnName))
				path.Super_column = superColumnName.GetBytes();

			GetClient().insert(
				_keyspace.KeyspaceName,
				key,
				path,
				value.GetBytes(),
				timestamp.UtcTicks,
				ConsistencyLevel.ONE
			);
		}

		/*
		 * remove(keyspace, key, column_path, timestamp, consistency_level)
		 */

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		public void Remove(IFluentColumn col)
		{
			Remove(col.GetPath());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="path"></param>
		public void Remove(FluentColumnPath path)
		{
			Remove(
				path.ColumnFamily,
				path.SuperColumn == null ? null : path.SuperColumn.Name,
				path.Column == null ? null : path.Column.Name
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		/// <param name="columnName"></param>
		/// <param name="superColumnName"></param>
		public void Remove(IFluentColumnFamily record, string superColumnName = null, string columnName = null)
		{
			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			Remove(record.Key, superColumnName, columnName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="key"></param>
		/// <param name="columnName"></param>
		public void Remove(string key, string superColumnName, string columnName)
		{
			var path = new ColumnPath {
				Column_family = FamilyName
			};

			if (!String.IsNullOrWhiteSpace(superColumnName))
				path.Super_column = superColumnName.GetBytes();

			if (!String.IsNullOrWhiteSpace(columnName))
				path.Column = columnName.GetBytes();

			GetClient().remove(
				_keyspace.KeyspaceName,
				key,
				path,
				DateTimeOffset.UtcNow.Ticks,
				ConsistencyLevel.ONE
			);
		}

		/*
		 * ColumnOrSuperColumn get(keyspace, key, column_path, consistency_level)
		 */

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		public IFluentColumn Get(IFluentColumn col)
		{
			return Get(col.GetPath());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="path"></param>
		public IFluentColumn Get(FluentColumnPath path)
		{
			return Get(
				path.ColumnFamily,
				path.SuperColumn == null ? null : path.SuperColumn.Name,
				path.Column == null ? null : path.Column.Name
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		/// <param name="columnName"></param>
		/// <param name="superColumnName"></param>
		public IFluentColumn Get(IFluentColumnFamily record, string superColumnName = null, string columnName = null)
		{
			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			return Get(record.Key, superColumnName, columnName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public IFluentColumn Get(string key, string superColumnName, string columnName)
		{
			var path = new ColumnPath {
				Column_family = FamilyName
			};

			if (!String.IsNullOrWhiteSpace(superColumnName))
				path.Super_column = superColumnName.GetBytes();

			if (!String.IsNullOrWhiteSpace(columnName))
				path.Column = columnName.GetBytes();

			var output = GetClient().get(
				_keyspace.KeyspaceName,
				key,
				path,
				ConsistencyLevel.ONE
			);

			return ObjectHelper.ConvertToFluentColumn(output);
		}

		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		/// <returns></returns>
		public IFluentColumnFamily GetSlice(IFluentColumnFamily record)
		{
			return GetSlice(record);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="parent"></param>
		public IFluentColumnFamily GetSlice(FluentColumnParent parent, IList<string> columnNames)
		{
			return GetSlice(
				parent.ColumnFamily,
				parent.SuperColumn == null ? null : parent.SuperColumn.Name,
				columnNames
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		/// <param name="columnName"></param>
		/// <param name="superColumnName"></param>
		public IFluentColumnFamily GetSlice(IFluentColumnFamily record, string superColumnName = null, IList<string> columnNames = null)
		{
			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			if (columnNames == null)
				columnNames = record.Select(x => x.Column.Name).ToList();

			return GetSlice(record.Key, superColumnName, columnNames);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public IFluentColumnFamily GetSlice(string key, string superColumnName, IList<string> columnNames)
		{
			var predicate = ObjectHelper.CreateSlicePredicate(columnNames);
			return GetSlice(key, superColumnName, predicate);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public IFluentColumnFamily GetSlice(string key, string superColumnName, object start, object finish, bool reversed = false, int count = 100)
		{
			var predicate = ObjectHelper.CreateSlicePredicate(start, finish, reversed, count);
			return GetSlice(key, superColumnName, predicate);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="predicate"></param>
		/// <returns></returns>
		protected IFluentColumnFamily GetSlice(string key, string superColumnName, SlicePredicate predicate)
		{
			var parent = new ColumnParent {
				Column_family = FamilyName
			};

			if (!String.IsNullOrWhiteSpace(superColumnName))
				parent.Super_column = superColumnName.GetBytes();

			var output = GetClient().get_slice(
				_keyspace.KeyspaceName,
				key,
				parent,
				predicate,
				ConsistencyLevel.ONE
			);

			var record = ObjectHelper.ConvertToFluentColumnFamily(key, FamilyName, superColumnName, output);

			return record;
		}
	}
}
