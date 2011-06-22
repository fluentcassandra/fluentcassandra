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

		public static List<byte[]> ToByteArrayList(this List<BytesType> list)
		{
			return list.Select(x => (byte[])x).ToList();
		}

		public static long ToTimestamp(this DateTimeOffset dt)
		{
			// this was changed from .NET Ticks to the Unix Epoch to be compatible with other cassandra libraries
			return Convert.ToInt64((DateTimeOffset.UtcNow - UnixStart).TotalMilliseconds);
		}

		public static IFluentBaseColumn<CompareWith> ConvertToFluentBaseColumn<CompareWith, CompareSubcolumnWith>(ColumnOrSuperColumn col)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			if (col.Super_column != null)
				return ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(col.Super_column);
			else if (col.Column != null)
				return ConvertColumnToFluentColumn<CompareWith>(col.Column);
			else
				return null;
		}

		public static FluentColumn<CompareWith> ConvertColumnToFluentColumn<CompareWith>(Column col)
			where CompareWith : CassandraType
		{
			return new FluentColumn<CompareWith> {
				ColumnName = CassandraType.GetType<CompareWith>(col.Name),
				ColumnValue = col.Value,
				ColumnTimestamp = new DateTimeOffset(col.Timestamp, TimeSpan.Zero),
				ColumnTimeToLive = col.Ttl
			};
		}

		public static FluentSuperColumn<CompareWith, CompareSubcolumnWith> ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(SuperColumn col)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superCol = new FluentSuperColumn<CompareWith, CompareSubcolumnWith> {
				ColumnName = CassandraType.GetType<CompareWith>(col.Name)
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
				var superColumn = col.Column.GetPath().SuperColumn.ColumnName;

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
				Name = column.ColumnName,
				Value = column.ColumnValue,
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
					Name = colY.ColumnName,
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
				Column_names = columnNames.Cast<byte[]>().ToList()
			};
		}

		public static SlicePredicate CreateSlicePredicate(byte[] start, byte[] finish, bool reversed = false, int count = 100)
		{
			return new SlicePredicate {
				Slice_range = new SliceRange {
					Start = start,
					Finish = finish,
					Reversed = reversed,
					Count = count
				}
			};
		}
	}
}
