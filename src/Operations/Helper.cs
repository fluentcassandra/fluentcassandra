using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Apache.Cassandra;
using FluentCassandra.Types;
using System.IO;
using System.IO.Compression;

namespace FluentCassandra.Operations
{
	internal static class Helper
	{
		public static List<byte[]> ToByteArrayList(List<CassandraObject> list)
		{
			return list.Select(x => x.TryToBigEndian()).ToList();
		}

		public static KeyRange CreateKeyRange(CassandraKeyRange range)
		{
			if (range.StartKey == null && range.EndKey == null && range.StartToken == null && range.EndToken == null)
				range = new CassandraKeyRange("", "", null, null, range.Count);

			return new KeyRange {
				Start_key = range.StartKey.TryToBigEndian(),
				End_key = range.EndKey.TryToBigEndian(),
				Start_token = range.StartToken,
				End_token = range.EndToken,
				Count = range.Count
			};
		}

		public static IndexClause CreateIndexClause(CassandraIndexClause index)
		{
			return new IndexClause {
				Start_key = index.StartKey.TryToBigEndian(),
				Count = index.Count,
				Expressions = index.CompiledExpressions
			};
		}

		public static ColumnParent CreateColumnParent(CassandraColumnParent parent)
		{
			return new ColumnParent {
				Column_family = parent.ColumnFamily,
				Super_column = parent.SuperColumn.TryToBigEndian()
			};
		}

		public static ColumnPath CreateColumnPath(CassandraColumnPath path)
		{
			return new ColumnPath {
				Column = path.Column.TryToBigEndian(),
				Column_family = path.ColumnFamily,
				Super_column = path.SuperColumn.TryToBigEndian()
			};
		}

		public static CassandraSlicePredicate SetSchemaForSlicePredicate(CassandraSlicePredicate predicate, CassandraColumnFamilySchema schema, bool forSuperColumn = false)
		{
			CassandraType columnType = forSuperColumn ? schema.SuperColumnNameType : schema.ColumnNameType;

			if (predicate is CassandraRangeSlicePredicate)
			{
				var x = (CassandraRangeSlicePredicate)predicate;
				var start = CassandraObject.GetCassandraObjectFromObject(x.Start, columnType);
				var finish = CassandraObject.GetCassandraObjectFromObject(x.Finish, columnType);

				return new CassandraRangeSlicePredicate(start, finish, x.Reversed, x.Count);
			}
			else if (predicate is CassandraColumnSlicePredicate)
			{
				var x = (CassandraColumnSlicePredicate)predicate;
				var cols = x.Columns.ToList();

				for (int i = 0; i < cols.Count; i++)
					cols[i] = CassandraObject.GetCassandraObjectFromObject(cols[i], columnType);

				return new CassandraColumnSlicePredicate(cols);
			}

			return null;
		}

		public static SlicePredicate CreateSlicePredicate(CassandraSlicePredicate predicate)
		{
			if (predicate is CassandraRangeSlicePredicate)
			{
				var x = (CassandraRangeSlicePredicate)predicate;
				return new SlicePredicate {
					Slice_range = new SliceRange {
						Start = x.Start.TryToBigEndian() ?? new byte[0],
						Finish = x.Finish.TryToBigEndian() ?? new byte[0],
						Reversed = x.Reversed,
						Count = x.Count
					}
				};
			}
			else if (predicate is CassandraColumnSlicePredicate)
			{
				var x = (CassandraColumnSlicePredicate)predicate;
				return new SlicePredicate {
					Column_names = x.Columns.Select(o => o.TryToBigEndian()).ToList()
				};
			}

			return null;
		}

		public static Column CreateColumn(CassandraColumn column)
		{
			var ccol = new Column {
				Name = column.Name.TryToBigEndian(),
				Value = column.Value.TryToBigEndian(),
				Timestamp = column.Timestamp.ToCassandraTimestamp()
			};

			if (column.Ttl.HasValue && column.Ttl.Value > 0)
				ccol.Ttl = column.Ttl.Value;

			return ccol;
		}

		public static CounterColumn CreateCounterColumn(CassandraCounterColumn column)
		{
			return new CounterColumn {
				Name = column.Name.TryToBigEndian(),
				Value = column.Value
			};
		}

		public static byte[] TryToBigEndian(this CassandraObject value)
		{
			if (value == null)
				return null;

			return value.ToBigEndian();
		}

		public static IFluentBaseColumn ConvertToFluentBaseColumn(ColumnOrSuperColumn col, CassandraColumnFamilySchema schema)
		{
			if (col.Super_column != null)
				return ConvertSuperColumnToFluentSuperColumn(col.Super_column, schema);
			else if (col.Column != null)
				return ConvertColumnToFluentColumn(col.Column, schema);
			else if (col.Counter_super_column != null)
				return ConvertColumnToFluentCounterColumn(col.Counter_column, schema);
			else if (col.Counter_column != null)
				return ConvertSuperColumnToFluentCounterSuperColumn(col.Counter_super_column, schema);
			else
				return null;
		}

		public static FluentCounterColumn ConvertColumnToFluentCounterColumn(CounterColumn col, CassandraColumnFamilySchema schema)
		{
			var colSchema = new CassandraColumnSchema();

			if (schema != null)
			{
				var name = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Name, schema.ColumnNameType);
				colSchema = schema.Columns.Where(x => x.Name == name).FirstOrDefault();

				if (colSchema == null)
				{
					colSchema = new CassandraColumnSchema();
					colSchema.NameType = schema.ColumnNameType;
					colSchema.Name = name;
					colSchema.ValueType = schema.DefaultColumnValueType;
				}
			}

			var fcol = new FluentCounterColumn(colSchema) {
				ColumnName = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Name, colSchema.NameType),
				ColumnValue = col.Value
			};

			return fcol;
		}

		public static FluentColumn ConvertColumnToFluentColumn(Column col, CassandraColumnFamilySchema schema)
		{
			var colSchema = new CassandraColumnSchema();

			if (schema != null)
			{
				var name = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Name, schema.ColumnNameType);
				colSchema = schema.Columns.Where(x => x.Name == name).FirstOrDefault();

				if (colSchema == null)
				{
					colSchema = new CassandraColumnSchema();
					colSchema.NameType = schema.ColumnNameType;
					colSchema.Name = name;
					colSchema.ValueType = schema.DefaultColumnValueType;
				}
			}

			return ConvertColumnToFluentColumn(col, colSchema);
		}

		public static FluentColumn ConvertColumnToFluentColumn(Column col, CassandraColumnSchema colSchema) 
		{
			colSchema = colSchema ?? new CassandraColumnSchema();

			var fcol = new FluentColumn(colSchema) {
				ColumnName = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Name, colSchema.NameType),
				ColumnValue = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Value, colSchema.ValueType),
				ColumnTimestamp = TimestampHelper.FromCassandraTimestamp(col.Timestamp),
			};

			if (col.__isset.ttl)
				fcol.ColumnSecondsUntilDeleted = col.Ttl;

			return fcol;
		}

		public static FluentSuperColumn ConvertSuperColumnToFluentCounterSuperColumn(CounterSuperColumn col, CassandraColumnFamilySchema schema)
		{
			var superColSchema = new CassandraColumnSchema {
				Name = col.Name
			};

			if (schema != null)
				superColSchema = new CassandraColumnSchema {
					NameType = schema.SuperColumnNameType,
					Name = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Name, schema.SuperColumnNameType),
					ValueType = schema.ColumnNameType
				};

			var superCol = new FluentSuperColumn(superColSchema) {
				ColumnName = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Name, superColSchema.NameType)
			};

			((ILoadable)superCol).BeginLoad();
			foreach (var xcol in col.Columns)
				superCol.Columns.Add(ConvertColumnToFluentCounterColumn(xcol, schema));
			((ILoadable)superCol).EndLoad();

			return superCol;
		}

		public static FluentSuperColumn ConvertSuperColumnToFluentSuperColumn(SuperColumn col, CassandraColumnFamilySchema schema)
		{
			var superColSchema = new CassandraColumnSchema {
				Name = col.Name
			};

			if (schema != null)
				superColSchema = new CassandraColumnSchema {
					NameType = schema.SuperColumnNameType,
					Name = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Name, schema.SuperColumnNameType),
					ValueType = schema.ColumnNameType
				};

			var superCol = new FluentSuperColumn(superColSchema) {
				ColumnName = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Name, superColSchema.NameType)
			};

			((ILoadable)superCol).BeginLoad();
			foreach (var xcol in col.Columns)
				superCol.Columns.Add(ConvertColumnToFluentColumn(xcol, schema));
			((ILoadable)superCol).EndLoad();

			return superCol;
		}

		public static IEnumerable<Mutation> CreateDeletedColumnMutation(IEnumerable<FluentMutation> mutation)
		{
			foreach (var col in mutation)
			{
				var deletion = new Deletion {
					Timestamp = col.ColumnTimestamp.ToCassandraTimestamp(),
					Predicate = CreateSlicePredicate(new[] { col.Column.ColumnName })
				};

				yield return new Mutation {
					Deletion = deletion
				};
			}
		}

		public static IEnumerable<Mutation> CreateDeletedSuperColumnMutation(IEnumerable<FluentMutation> mutation)
		{
			foreach (var col in mutation)
			{
				var superColumn = col.Column.GetPath().SuperColumn.ColumnName.TryToBigEndian();

				var deletion = new Deletion {
					Timestamp = col.ColumnTimestamp.ToCassandraTimestamp(),
					Super_column = superColumn,
					Predicate = CreateSlicePredicate(new[] { col.Column.ColumnName })
				};

				yield return new Mutation {
					Deletion = deletion
				};
			}
		}

		public static Mutation CreateInsertedOrChangedMutation(FluentMutation mutation)
		{
			switch (mutation.Type)
			{
				case MutationType.Added:
				case MutationType.Changed:
					var column = mutation.Column;

					if (column is FluentColumn)
					{
						return new Mutation {
							Column_or_supercolumn = new ColumnOrSuperColumn {
								Column = CreateColumn((FluentColumn)column)
							}
						};
					}
					else if (column is FluentSuperColumn)
					{
						var colY = (FluentSuperColumn)column;
						var superColumn = new SuperColumn {
							Name = colY.ColumnName.TryToBigEndian(),
							Columns = new List<Column>()
						};

						foreach (var col in colY.MutationTracker.GetMutations().Select(x => x.Column).OfType<FluentColumn>())
							superColumn.Columns.Add(CreateColumn(col));

						return new Mutation {
							Column_or_supercolumn = new ColumnOrSuperColumn {
								Super_column = superColumn
							}
						};
					}
					break;
			}

			return null;
		}

		public static Column CreateColumn(FluentColumn column)
		{
			var col = new Column {
				Name = column.ColumnName.TryToBigEndian(),
				Value = column.ColumnValue.TryToBigEndian(),
				Timestamp = column.ColumnTimestamp.ToCassandraTimestamp()
			};

			if (column.ColumnSecondsUntilDeleted.HasValue)
				col.Ttl = column.ColumnSecondsUntilDeleted.Value;

			return col;
		}

		public static ColumnOrSuperColumn CreateColumnOrSuperColumn(IFluentBaseColumn column)
		{
			if (column is FluentColumn)
			{
				return new ColumnOrSuperColumn {
					Column = CreateColumn((FluentColumn)column)
				};
			}
			else if (column is FluentSuperColumn)
			{
				var colY = (FluentSuperColumn)column;
				var superColumn = new SuperColumn {
					Name = colY.ColumnName.TryToBigEndian(),
					Columns = new List<Column>()
				};

				foreach (var col in colY.Columns.OfType<FluentColumn>())
					superColumn.Columns.Add(CreateColumn(col));

				return new ColumnOrSuperColumn {
					Super_column = superColumn
				};
			}
			else
			{
				return null;
			}
		}

		public static SlicePredicate CreateSlicePredicate(IEnumerable<CassandraObject> columnNames)
		{
			return new SlicePredicate {
				Column_names = columnNames.Select(o => o.TryToBigEndian()).ToList()
			};
		}

		public static byte[] ZlibCompress(byte[] data)
		{
			// Note: even though Cassandra calls this GZIP compression it is actually ZLIB compression provided by the
			// Java Inflator class http://docs.oracle.com/javase/1.4.2/docs/api/java/util/zip/Inflater.html
			// If it wasn't for this post explaining how to get Deflator to mimic ZLIB compression none of the following
			// code would work http://tlzprgmr.wordpress.com/2010/03/17/net-deflatestreamzlib-compatibility/
			using (MemoryStream outStream = new MemoryStream())
			{
				// zlib header
				outStream.WriteByte(0x58);
				outStream.WriteByte(0x85);

				// zlib body
				using (var compressor = new DeflateStream(outStream, CompressionMode.Compress, true))
					compressor.Write(data, 0, data.Length);

				// zlib checksum - a naive implementation of adler-32 checksum
				const uint A32Mod = 65521;
				uint s1 = 1, s2 = 0;
				foreach (byte b in data)
				{
					s1 = (s1 + b) % A32Mod;
					s2 = (s2 + s1) % A32Mod;
				}

				int adler32 = unchecked((int)((s2 << 16) + s1));
				outStream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(adler32)), 0, sizeof(uint));

				// zlib compatible compressed query
				var bytes = outStream.ToArray();
				outStream.Close();

				return bytes;
			}
		}
	}
}
