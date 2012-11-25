﻿using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraClientWrapper //: Apache.Cassandra.Cassandra.Iface
	{
		private readonly Apache.Cassandra.Cassandra.Iface _client;

		public CassandraClientWrapper(Apache.Cassandra.Cassandra.Iface client)
		{
			_client = client;
		}

		public void login(Apache.Cassandra.AuthenticationRequest auth_request)
		{
			_client.login(auth_request);
		}

		public void set_keyspace(string keyspace)
		{
			_client.set_keyspace(keyspace);
		}

		public Apache.Cassandra.ColumnOrSuperColumn get(CassandraObject key, CassandraColumnPath column_path, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			return _client.get(
				key.TryToBigEndian(),
				Helper.CreateColumnPath(column_path),
				consistency_level);
		}

		public List<Apache.Cassandra.ColumnOrSuperColumn> get_slice(CassandraObject key, CassandraColumnParent column_parent, CassandraSlicePredicate predicate, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			return _client.get_slice(
				key.TryToBigEndian(),
				Helper.CreateColumnParent(column_parent),
				Helper.CreateSlicePredicate(predicate),
				consistency_level);
		}

		public int get_count(CassandraObject key, CassandraColumnParent column_parent, CassandraSlicePredicate predicate, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			return _client.get_count(
				key.TryToBigEndian(),
				Helper.CreateColumnParent(column_parent),
				Helper.CreateSlicePredicate(predicate),
				consistency_level);
		}

		public Dictionary<byte[], List<Apache.Cassandra.ColumnOrSuperColumn>> multiget_slice(List<CassandraObject> keys, CassandraColumnParent column_parent, CassandraSlicePredicate predicate, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			return _client.multiget_slice(
				Helper.ToByteArrayList(keys),
				Helper.CreateColumnParent(column_parent),
				Helper.CreateSlicePredicate(predicate),
				consistency_level);
		}

		public Dictionary<byte[], int> multiget_count(List<CassandraObject> keys, CassandraColumnParent column_parent, CassandraSlicePredicate predicate, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			return _client.multiget_count(
				Helper.ToByteArrayList(keys),
				Helper.CreateColumnParent(column_parent),
				Helper.CreateSlicePredicate(predicate),
				consistency_level);
		}

		public List<Apache.Cassandra.KeySlice> get_range_slices(CassandraColumnParent column_parent, CassandraSlicePredicate predicate, CassandraKeyRange range, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
        	return _client.get_range_slices(
				Helper.CreateColumnParent(column_parent),
				Helper.CreateSlicePredicate(predicate),
				Helper.CreateKeyRange(range),
				consistency_level);
		}

		public List<Apache.Cassandra.KeySlice> get_indexed_slices(CassandraColumnParent column_parent, CassandraIndexClause index_clause, CassandraSlicePredicate column_predicate, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			return _client.get_indexed_slices(
				Helper.CreateColumnParent(column_parent),
				Helper.CreateIndexClause(index_clause),
				Helper.CreateSlicePredicate(column_predicate),
				consistency_level);
		}

		public void insert(CassandraObject key, CassandraColumnParent column_parent, CassandraColumn column, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			_client.insert(
				key.TryToBigEndian(),
				Helper.CreateColumnParent(column_parent),
				Helper.CreateColumn(column),
				consistency_level);
		}

		public void add(CassandraObject key, CassandraColumnParent column_parent, CassandraCounterColumn column, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			_client.add(
				key.TryToBigEndian(),
				Helper.CreateColumnParent(column_parent),
				Helper.CreateCounterColumn(column),
				consistency_level);
		}

		public void remove(CassandraObject key, CassandraColumnPath column_path, long timestamp, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			_client.remove(
				key.TryToBigEndian(),
				Helper.CreateColumnPath(column_path),
				timestamp,
				consistency_level);
		}

		public void remove_counter(CassandraObject key, CassandraColumnPath path, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			_client.remove_counter(
				key.TryToBigEndian(),
				Helper.CreateColumnPath(path),
				consistency_level);
		}

		public void batch_mutate(Dictionary<byte[], Dictionary<string, List<Apache.Cassandra.Mutation>>> mutation_map, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			_client.batch_mutate(mutation_map, consistency_level);
		}

		public void truncate(string cfname)
		{
			_client.truncate(cfname);
		}

		public Dictionary<string, List<string>> describe_schema_versions()
		{
			return _client.describe_schema_versions();
		}

		public List<Apache.Cassandra.KsDef> describe_keyspaces()
		{
			return _client.describe_keyspaces();
		}

		public string describe_cluster_name()
		{
			return _client.describe_cluster_name();
		}

		public string describe_version()
		{
			return _client.describe_version();
		}

		public List<Apache.Cassandra.TokenRange> describe_ring(string keyspace)
		{
			return _client.describe_ring(keyspace);
		}

		public string describe_partitioner()
		{
			return _client.describe_partitioner();
		}

		public string describe_snitch()
		{
			return _client.describe_snitch();
		}

		public Apache.Cassandra.KsDef describe_keyspace(string keyspace)
		{
			return _client.describe_keyspace(keyspace);
		}

		public List<string> describe_splits(string cfName, string start_token, string end_token, int keys_per_split)
		{
			return _client.describe_splits(cfName, start_token, end_token, keys_per_split);
		}

		public string system_add_column_family(Apache.Cassandra.CfDef cf_def)
		{
			return _client.system_add_column_family(cf_def);
		}

		public string system_drop_column_family(string column_family)
		{
			return _client.system_drop_column_family(column_family);
		}

		public string system_add_keyspace(Apache.Cassandra.KsDef ks_def)
		{
			return _client.system_add_keyspace(ks_def);
		}

		public string system_drop_keyspace(string keyspace)
		{
			return _client.system_drop_keyspace(keyspace);
		}

		public string system_update_keyspace(Apache.Cassandra.KsDef ks_def)
		{
			return _client.system_update_keyspace(ks_def);
		}

		public string system_update_column_family(Apache.Cassandra.CfDef cf_def)
		{
			return _client.system_update_column_family(cf_def);
		}

		public Apache.Cassandra.CqlResult execute_cql_query(byte[] query, Apache.Cassandra.Compression compression)
		{
			return _client.execute_cql_query(query, compression);
		}

		#region version 1.1.0

		public List<Apache.Cassandra.KeySlice> get_paged_slice(string column_family, CassandraKeyRange range, CassandraObject start_column, Apache.Cassandra.ConsistencyLevel consistency_level)
		{
			return _client.get_paged_slice(
				column_family,
				Helper.CreateKeyRange(range),
				start_column.TryToBigEndian(),
				consistency_level);
		}

		public Apache.Cassandra.CqlPreparedResult prepare_cql_query(byte[] query, Apache.Cassandra.Compression compression)
		{
			return _client.prepare_cql_query(query, compression);
		}

		public Apache.Cassandra.CqlResult execute_prepared_cql_query(int itemId, List<byte[]> values)
		{
			return _client.execute_prepared_cql_query(itemId, values);
		}

		public void set_cql_version(string version)
		{
			_client.set_cql_version(version);
		}

		#endregion

		#region version 1.1.1

		public Dictionary<string, string> describe_token_map()
		{
			return _client.describe_token_map();
		}

		#endregion
	}
}