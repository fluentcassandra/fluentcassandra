using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Configuration;
using System.Collections;
using FluentCassandra.Types;

namespace FluentCassandra
{
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public class CassandraColumnFamily
	{
		private CassandraContext _context;
		private CassandraKeyspace _keyspace;
		private IConnection _connection;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public CassandraColumnFamily(CassandraContext context, CassandraKeyspace keyspace, IConnection connection, string columnFamily)
		{
			_context = context;
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
		/// If set contains the super column name that we are querying against.
		/// </summary>
		public CassandraType SuperColumnName { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		internal Cassandra.Client GetClient()
		{
			return _connection.Client;
		}

		/// <summary>
		/// Verifies that the family passed in is part of this family.
		/// </summary>
		/// <param name="family"></param>
		/// <returns></returns>
		protected bool IsPartOfFamily(IFluentColumnFamily family)
		{
			return String.Equals(family.FamilyName, FamilyName);
		}

		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnNames"></param>
		/// <returns></returns>
		public IFluentColumnFamily GetSingle(string key, List<T> columnNames)
		{
			return GetSingle(
				key,
				(byte[])null,
				columnNames
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="parent"></param>
		public IFluentColumnFamily GetSingle(string key, FluentColumnParent parent, List<T> columnNames)
		{
			var record = parent.ColumnFamily;

			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			return GetSingle(
				key,
				parent.SuperColumn == null ? null : parent.SuperColumn.GetNameBytes(),
				columnNames
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public IFluentColumnFamily GetSingle(string key, T superColumnName, List<T> columnNames)
		{
			var predicate = ObjectHelper.CreateSlicePredicate(columnNames);
			return GetSingle(key, superColumnName, predicate);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="parent"></param>
		public IFluentColumnFamily GetSingle(string key, FluentColumnParent parent, T start, T finish, bool reversed = false, int count = 100)
		{
			var record = parent.ColumnFamily;

			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			return GetSingle(
				key,
				parent.SuperColumn == null ? null : parent.SuperColumn.GetNameBytes(),
				start,
				finish,
				reversed,
				count
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public IFluentColumnFamily GetSingle(string key, T superColumnName, T start, T finish, bool reversed = false, int count = 100)
		{
			var predicate = ObjectHelper.CreateSlicePredicate(start, finish, reversed, count);
			return GetSingle(key, superColumnName, predicate);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="predicate"></param>
		/// <returns></returns>
		protected IFluentColumnFamily GetSingle(string key, byte[] superColumnName, SlicePredicate predicate)
		{
			var parent = new ColumnParent {
				Column_family = FamilyName
			};

			if (superColumnName != null)
				parent.Super_column = superColumnName;

			var output = GetClient().get_slice(
				_keyspace.KeyspaceName,
				key,
				parent,
				predicate,
				ConsistencyLevel.ONE
			);

			var record = ObjectHelper.ConvertToFluentColumnFamily(key, FamilyName, superColumnName, output);
			_context.Attach(record);
			record.MutationTracker.Clear();
			return record;
		}

		/*
		 * map<string,list<ColumnOrSuperColumn>> multiget_slice(keyspace, keys, column_parent, predicate, consistency_level)
		 */

		/// <summary>
		/// 
		/// </summary>
		/// <param name="parent"></param>
		public IEnumerable<IFluentColumnFamily> Get(IEnumerable<string> keys, FluentColumnParent parent, List<T> columnNames)
		{
			var record = parent.ColumnFamily;

			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			return Get(
				keys,
				parent.SuperColumn == null ? null : parent.SuperColumn.GetNameBytes(),
				columnNames
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public IEnumerable<IFluentColumnFamily> Get(IEnumerable<string> keys, byte[] superColumnName, List<T> columnNames)
		{
			var keysList = keys is List<string> ? (List<string>)keys : keys.ToList();

			var predicate = ObjectHelper.CreateSlicePredicate(columnNames);
			return Get(keysList, superColumnName, predicate);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="parent"></param>
		public IEnumerable<IFluentColumnFamily> Get(IEnumerable<string> keys, FluentColumnParent parent, T start, T finish, bool reversed = false, int count = 100)
		{
			var record = parent.ColumnFamily;

			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			return Get(
				keys,
				parent.SuperColumn == null ? null : parent.SuperColumn.GetNameBytes(),
				start,
				finish,
				reversed,
				count
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public IEnumerable<IFluentColumnFamily> Get(IEnumerable<string> keys, T superColumnName, T start, T finish, bool reversed = false, int count = 100)
		{
			var keysList = keys is List<string> ? (List<string>)keys : keys.ToList();

			var predicate = ObjectHelper.CreateSlicePredicate(start, finish, reversed, count);
			return Get(keysList, superColumnName, predicate);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keys"></param>
		/// <param name="superColumnName"></param>
		/// <param name="predicate"></param>
		/// <returns></returns>
		protected IEnumerable<IFluentColumnFamily> Get(List<string> keys, T superColumnName, SlicePredicate predicate)
		{
			var parent = new ColumnParent {
				Column_family = FamilyName
			};

			if (superColumnName != null)
				parent.Super_column = superColumnName;

			var output = GetClient().multiget_slice(
				_keyspace.KeyspaceName,
				keys,
				parent,
				predicate,
				ConsistencyLevel.ONE
			);

			foreach (var record in output)
			{
				var family = ObjectHelper.ConvertToFluentColumnFamily(record.Key, FamilyName, superColumnName, record.Value);
				_context.Attach(family);
				family.MutationTracker.Clear();
				yield return family;
			}
		}

		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyRange"></param>
		/// <param name="parent"></param>
		/// <param name="columnNames"></param>
		/// <returns></returns>
		public IEnumerable<IFluentColumnFamily> GetRange(CassandraKeyRange keyRange, FluentColumnParent parent, List<T> columnNames)
		{
			var record = parent.ColumnFamily;

			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			return GetRange(
				keyRange,
				parent.SuperColumn == null ? null : parent.SuperColumn.GetNameBytes(),
				columnNames
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyRange"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnNames"></param>
		/// <returns></returns>
		public IEnumerable<IFluentColumnFamily> GetRange(CassandraKeyRange keyRange, T superColumnName, List<T> columnNames)
		{
			var predicate = ObjectHelper.CreateSlicePredicate(columnNames);
			return GetRange(ObjectHelper.CreateKeyRange(keyRange), superColumnName, predicate);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyRange"></param>
		/// <param name="parent"></param>
		/// <param name="start"></param>
		/// <param name="finish"></param>
		/// <param name="reversed"></param>
		/// <param name="count"></param>
		/// <returns></returns>
		public IEnumerable<IFluentColumnFamily> GetRange(CassandraKeyRange keyRange, FluentColumnParent parent, T start, T finish, bool reversed = false, int count = 100)
		{
			var record = parent.ColumnFamily;

			if (IsPartOfFamily(record))
				throw new FluentCassandraException("The record passed in is not part of this family.");

			return GetRange(
				keyRange,
				parent.SuperColumn == null ? null : parent.SuperColumn.GetNameBytes(),
				start,
				finish,
				reversed,
				count
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyRange"></param>
		/// <param name="superColumnName"></param>
		/// <param name="start"></param>
		/// <param name="finish"></param>
		/// <param name="reversed"></param>
		/// <param name="count"></param>
		/// <returns></returns>
		public IEnumerable<IFluentColumnFamily> GetRange(CassandraKeyRange keyRange, T superColumnName, T start, T finish, bool reversed = false, int count = 100)
		{
			var predicate = ObjectHelper.CreateSlicePredicate(start, finish, reversed, count);
			return GetRange(ObjectHelper.CreateKeyRange(keyRange), superColumnName, predicate);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyRange"></param>
		/// <param name="superColumnName"></param>
		/// <param name="predicate"></param>
		/// <returns></returns>
		protected IEnumerable<IFluentColumnFamily> GetRange(KeyRange keyRange, T superColumnName, SlicePredicate predicate)
		{
			var parent = new ColumnParent {
				Column_family = FamilyName
			};

			if (superColumnName != null)
				parent.Super_column = superColumnName;

			var output = GetClient().get_range_slices(
				_keyspace.KeyspaceName,
				parent,
				predicate,
				keyRange,
				ConsistencyLevel.ONE
			);

			foreach (var record in output)
			{
				var family = ObjectHelper.ConvertToFluentColumnFamily(record.Key, FamilyName, superColumnName, record.Columns);
				_context.Attach(family);
				family.MutationTracker.Clear();
				yield return family;
			}
		}
	}
}
