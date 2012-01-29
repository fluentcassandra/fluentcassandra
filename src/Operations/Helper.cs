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

		public static List<byte[]> ToByteArrayList(List<BytesType> list)
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
			if (predicate is RangeSlicePredicate)
			{
				var x = (RangeSlicePredicate)predicate;
				return new SlicePredicate {
					Slice_range = new SliceRange {
						Start = x.Start.TryToBigEndian() ?? new byte[0],
						Finish = x.Finish.TryToBigEndian() ?? new byte[0],
						Reversed = x.Reversed,
						Count = x.Count
					}
				};
			}
			else if (predicate is ColumnSlicePredicate)
			{
				var x = (ColumnSlicePredicate)predicate;
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

		public static byte[] TryToBigEndian(this CassandraType value)
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

		public static IFluentBaseColumn<CompareWith> ConvertToFluentBaseColumn<CompareWith, CompareSubcolumnWith>(ColumnOrSuperColumn col)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			if (col.Super_column != null)
				return ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(col.Super_column);
			else if (col.Column != null)
				return ConvertColumnToFluentColumn<CompareWith>(col.Column);
			else if (col.Counter_super_column != null)
				throw new NotSupportedException("Reading CounterSuperColumns isn't supported yet.");
			else if (col.Counter_column != null)
				throw new NotSupportedException("Reading CounterSuperColumns isn't supported yet.");
			else
				return null;
		}

		public static FluentColumn<CompareWith> ConvertColumnToFluentColumn<CompareWith>(Column col)
			where CompareWith : CassandraType
		{

			var fcol = new FluentColumn<CompareWith> {
				ColumnName = CassandraType.GetTypeFromDatabaseValue<CompareWith>(col.Name),
				ColumnValue = CassandraType.GetTypeFromDatabaseValue<BytesType>(col.Value),
				ColumnTimestamp = new DateTimeOffset(col.Timestamp, TimeSpan.Zero),
			};

			if (col.__isset.ttl)
				fcol.ColumnSecondsUntilDeleted = col.Ttl;

			return fcol;
		}

		public static FluentSuperColumn<CompareWith, CompareSubcolumnWith> ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(SuperColumn col)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superCol = new FluentSuperColumn<CompareWith, CompareSubcolumnWith> {
				ColumnName = CassandraType.GetTypeFromDatabaseValue<CompareWith>(col.Name)
			};

			foreach (var xcol in col.Columns)
				superCol.Columns.Add(ConvertColumnToFluentColumn<CompareSubcolumnWith>(xcol));

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

		public static Column CreateColumn(IFluentColumn column)
		{
			return new Column {
				Name = column.ColumnName.TryToBigEndian(),
				Value = column.ColumnValue.TryToBigEndian(),
				Timestamp = column.ColumnTimestamp.ToTimestamp()
			};
		}

		public static ColumnOrSuperColumn CreateColumnOrSuperColumn(IFluentBaseColumn column)
		{
			if (column is IFluentColumn)
			{
				return new ColumnOrSuperColumn {
					Column = CreateColumn((IFluentColumn)column)
				};
			}
			else if (column is IFluentSuperColumn)
			{
				var colY = (IFluentSuperColumn)column;
				var superColumn = new SuperColumn {
					Name = colY.ColumnName.TryToBigEndian(),
					Columns = new List<Column>()
				};

				foreach (var col in colY.Columns.OfType<IFluentColumn>())
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

		public static SlicePredicate CreateSlicePredicate(IEnumerable<CassandraType> columnNames)
		{
			return new SlicePredicate {
				Column_names = columnNames.Select(o => o.TryToBigEndian()).ToList()
			};
		}
	}
}
