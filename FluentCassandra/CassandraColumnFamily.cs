using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Configuration;
using System.Collections;

namespace FluentCassandra
{
	/// <seealso href=""/>
	public class CassandraColumnFamily
	{
		private CassandraKeyspace _keyspace;
		private IConnection _connection;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public CassandraColumnFamily(CassandraKeyspace keyspace, IConnection connection)
		{
			_keyspace = keyspace;
			_connection = connection;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		protected Cassandra.Client GetClient()
		{
			return _connection.Client;
		}

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
			return ColumnCount(record.Key, record.Name, superColumnName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <returns></returns>
		public int ColumnCount(string key, string columnFamily, string superColumnName)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily
			};

			if (!String.IsNullOrWhiteSpace(superColumnName))
				parent.Super_column = superColumnName.GetBytes();

			return GetClient().get_count(
				_keyspace.KeyspaceName,
				key,
				parent,
				ConsistencyLevel.ONE
			);
		}

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
		/// <param name="record"></param>
		public void Insert(IFluentColumnFamily record)
		{
			foreach (var col in record)
				Insert(col);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="path"></param>
		public void Insert(FluentColumnPath path)
		{
			Insert(
				path.ColumnFamily.Key,
				path.ColumnFamily.Name,
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
		public void Insert(string key, string columnFamily, string superColumnName, FluentColumn col)
		{
			Insert(
				key,
				columnFamily,
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
		public void Insert(string key, string columnFamily, string superColumnName, string columnName, object value, DateTimeOffset timestamp)
		{
			var path = new ColumnPath {
				Column_family = columnFamily,
				Column = columnName.GetBytes()
			};

			if (!String.IsNullOrWhiteSpace(superColumnName))
				path.Super_column = superColumnName.GetBytes();

			GetClient().insert(
				_keyspace.KeyspaceName,
				key,
				path,
				value.GetBytes(),
				timestamp.Ticks,
				ConsistencyLevel.ONE
			);
		}

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
			Remove(record.Key, record.Name, superColumnName, columnName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="key"></param>
		/// <param name="columnName"></param>
		public void Remove(string key, string columnFamily, string superColumnName, string columnName)
		{
			var path = new ColumnPath {
				Column_family = columnFamily
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
			return Get(record.Key, record.Name, superColumnName, columnName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public IFluentColumn Get(string key, string columnFamily, string superColumnName, string columnName)
		{
			var path = new ColumnPath {
				Column_family = columnFamily
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

			return ConvertToFluentColumn(output);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="path"></param>
		public IFluentColumnFamily GetSlice(FluentColumnParent path, IList<string> columnNames)
		{
			return GetSlice(
				path.ColumnFamily,
				columnNames,
				path.SuperColumn == null ? null : path.SuperColumn.Name
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		/// <param name="columnName"></param>
		/// <param name="superColumnName"></param>
		public IFluentColumnFamily GetSlice(IFluentColumnFamily record, IList<string> columnNames, string superColumnName = null)
		{
			return GetSlice(record.Key, record.Name, superColumnName, columnNames);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public IFluentColumnFamily GetSlice(string key, string columnFamily, string superColumnName, IList<string> columnNames)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily
			};

			if (!String.IsNullOrWhiteSpace(superColumnName))
				parent.Super_column = superColumnName.GetBytes();

			var predicate = new SlicePredicate {
				Column_names = columnNames.Select(x => x.GetBytes()).ToList()
			};

			var output = GetClient().get_slice(
				_keyspace.KeyspaceName,
				key,
				parent,
				predicate,
				ConsistencyLevel.ONE
			);

			var family = ConvertToFluentColumnFamily(output);
			family.Key = key;
			family.Name = columnFamily;

			return family;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="cols"></param>
		/// <returns></returns>
		protected IFluentColumnFamily ConvertToFluentColumnFamily(List<ColumnOrSuperColumn> cols)
		{
			var sample = cols.FirstOrDefault();

			if (sample == null)
				return null;

			var fluentSample = ConvertToFluentColumn(sample);

			if (fluentSample is FluentColumn)
				return ConvertColumnListToFluentColumnFamily(cols.Select(x => x.Column).ToList());
			else if (fluentSample is FluentSuperColumn)
				return ConvertSuperColumnListToFluentSuperColumnFamily(cols.Select(x => x.Super_column).ToList());
			else
				return null;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="cols"></param>
		/// <returns></returns>
		private FluentColumnFamily ConvertColumnListToFluentColumnFamily(List<Column> cols)
		{
			var family = new FluentColumnFamily();

			foreach (var col in cols)
				family.Columns.Add(ConvertColumnToFluentColumn(col));

			return family;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="cols"></param>
		/// <returns></returns>
		private FluentSuperColumnFamily ConvertSuperColumnListToFluentSuperColumnFamily(List<SuperColumn> cols)
		{
			var family = new FluentSuperColumnFamily();

			foreach (var col in cols)
				family.Columns.Add(ConverSuperColumnToFluentSuperColumn(col));

			return family;
		}


		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		protected IFluentColumn ConvertToFluentColumn(ColumnOrSuperColumn col)
		{
			if (col.Super_column != null)
				return ConverSuperColumnToFluentSuperColumn(col.Super_column);
			else if (col.Column != null)
				return ConvertColumnToFluentColumn(col.Column);
			else
				return null;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		private FluentColumn ConvertColumnToFluentColumn(Column col)
		{
			return new FluentColumn {
				Name = col.Name.GetObject<string>(),
				ValueBytes = col.Value,
				Timestamp = new DateTimeOffset(col.Timestamp, TimeSpan.Zero)
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		private FluentSuperColumn ConverSuperColumnToFluentSuperColumn(SuperColumn col)
		{
			var superCol = new FluentSuperColumn() {
				Name = col.Name.GetObject<string>()
			};

			foreach (var xcol in col.Columns)
				superCol.Columns.Add(ConvertColumnToFluentColumn(xcol));

			return superCol;
		}
	}
}
