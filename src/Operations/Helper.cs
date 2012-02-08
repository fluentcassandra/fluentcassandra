using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	internal static class Helper
	{
		private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

		public static List<byte[]> ToByteArrayList(List<CassandraObject> list)
		{
			return list.Select(x => x.TryToBigEndian()).ToList();
		}

		public static KeyRange CreateKeyRange(CassandraKeyRange range)
		{
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
				Timestamp = column.Timestamp.ToTimestamp()
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

		public static long ToTimestamp(this DateTimeOffset dt)
		{
			// this was changed from .NET Ticks to the Unix Epoch to be compatible with other cassandra libraries
			return Convert.ToInt64((dt - UnixStart).TotalMilliseconds);
		}

		public static IFluentBaseColumn ConvertToFluentBaseColumn(ColumnOrSuperColumn col, CassandraColumnFamilySchema schema = null)
		{
			if (col.Super_column != null)
				return ConvertSuperColumnToFluentSuperColumn(col.Super_column, schema);
			else if (col.Column != null)
				return ConvertColumnToFluentColumn(col.Column, schema);
			else if (col.Counter_super_column != null)
				throw new NotSupportedException("Reading CounterSuperColumns isn't supported yet.");
			else if (col.Counter_column != null)
				throw new NotSupportedException("Reading CounterSuperColumns isn't supported yet.");
			else
				return null;
		}

		public static FluentColumn ConvertColumnToFluentColumn(Column col, CassandraColumnFamilySchema schema = null)
		{
			var colSchema = new CassandraColumnSchema();

			if (schema != null)
			{
				colSchema = schema.Columns.Where(x => x.Name == col.Name).FirstOrDefault();

				if (colSchema == null)
				{
					colSchema = new CassandraColumnSchema();
					colSchema.NameType = schema.ColumnNameType;
				}
			}

			var fcol = new FluentColumn(colSchema) {
				ColumnName = CassandraObject.GetTypeFromDatabaseValue(col.Name, colSchema.NameType),
				ColumnValue = CassandraObject.GetTypeFromDatabaseValue(col.Value, colSchema.ValueType),
				ColumnTimestamp = UnixStart.AddMilliseconds(col.Timestamp),
			};

			if (col.__isset.ttl)
				fcol.ColumnSecondsUntilDeleted = col.Ttl;

			return fcol;
		}

		public static FluentSuperColumn ConvertSuperColumnToFluentSuperColumn(SuperColumn col, CassandraColumnFamilySchema schema = null)
		{
			var nameType = CassandraType.BytesType;

			if (schema != null)
			{
				nameType = schema.SuperColumnNameType;
			}

			var superCol = new FluentSuperColumn {
				ColumnName = CassandraObject.GetTypeFromDatabaseValue(col.Name, nameType)
			};

			foreach (var xcol in col.Columns)
				superCol.Columns.Add(ConvertColumnToFluentColumn(xcol, schema));

			return superCol;
		}

		public static IEnumerable<Mutation> CreateDeletedColumnMutation(IEnumerable<FluentMutation> mutation)
		{
			foreach (var col in mutation)
			{
				var deletion = new Deletion {
					Timestamp = col.Timestamp.ToTimestamp(),
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
					Timestamp = col.Timestamp.ToTimestamp(),
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
					return new Mutation {
						Column_or_supercolumn = CreateColumnOrSuperColumn(mutation.Column)
					};

				default:
					return null;
			}
		}

		public static Column CreateColumn(FluentColumn column)
		{
			return new Column {
				Name = column.ColumnName.TryToBigEndian(),
				Value = column.ColumnValue.TryToBigEndian(),
				Timestamp = column.ColumnTimestamp.ToTimestamp()
			};
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
	}
}
