using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra
{
	internal static class ObjectHelper
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="cols"></param>
		/// <returns></returns>
		public static IFluentColumnFamily ConvertToFluentColumnFamily(string key, string columnFamily, object superColumnName, List<ColumnOrSuperColumn> cols)
		{
			var sample = cols.FirstOrDefault();

			if (sample == null)
				return null;

			var fluentSample = ConvertToFluentColumn(sample);

			if (fluentSample is FluentColumn)
			{
				var record = ConvertColumnListToFluentColumnFamily(
					key,
					columnFamily,
					cols.Select(x => x.Column).ToList()
				);

				return record;
			}
			else if (fluentSample is FluentSuperColumn)
			{
				var record = ConvertSuperColumnListToFluentSuperColumnFamily(
					key,
					columnFamily,
					cols.Select(x => x.Super_column).ToList()
				);

				return record;
			}
			else
				return null;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="cols"></param>
		/// <returns></returns>
		public static FluentColumnFamily ConvertColumnListToFluentColumnFamily(string key, string columnFamily, List<Column> cols)
		{
			var family = new FluentColumnFamily(key, columnFamily);

			foreach (var col in cols)
				family.Columns.Add(ConvertColumnToFluentColumn(col));

			return family;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="cols"></param>
		/// <returns></returns>
		public static FluentSuperColumnFamily ConvertSuperColumnListToFluentSuperColumnFamily(string key, string columnFamily, List<SuperColumn> cols)
		{
			var family = new FluentSuperColumnFamily(key, columnFamily);

			foreach (var col in cols)
				family.Columns.Add(ConverSuperColumnToFluentSuperColumn(col));

			return family;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		public static IFluentColumn ConvertToFluentColumn(ColumnOrSuperColumn col)
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
		public static FluentColumn ConvertColumnToFluentColumn(Column col)
		{
			return new FluentColumn {
				NameBytes = col.Name,
				ValueBytes = col.Value,
				Timestamp = new DateTimeOffset(col.Timestamp, TimeSpan.Zero)
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		public static FluentSuperColumn ConverSuperColumnToFluentSuperColumn(SuperColumn col)
		{
			var superCol = new FluentSuperColumn() {
				NameBytes = col.Name
			};

			foreach (var xcol in col.Columns)
				superCol.Columns.Add(ConvertColumnToFluentColumn(xcol));

			return superCol;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mutation"></param>
		/// <returns></returns>
		public static Mutation CreateDeletedColumnMutation(IEnumerable<FluentMutation> mutation)
		{
			var columnNames = mutation.Select(m => m.Column.GetNameBytes()).ToList();

			var deletion = new Deletion {
				Timestamp = DateTimeOffset.UtcNow.UtcTicks,
				Predicate = CreateSlicePredicate(columnNames)
			};

			return new Mutation {
				Deletion = deletion
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mutation"></param>
		/// <returns></returns>
		public static Mutation CreateDeletedSuperColumnMutation(IEnumerable<FluentMutation> mutation)
		{
			var superColumn = mutation.Select(m => m.Column.SuperColumn.GetNameBytes()).FirstOrDefault();
			var columnNames = mutation.Select(m => m.Column.GetNameBytes()).ToList();

			var deletion = new Deletion {
				Timestamp = DateTimeOffset.UtcNow.UtcTicks,
				Super_column = superColumn,
				Predicate = CreateSlicePredicate(columnNames)
			};

			return new Mutation {
				Deletion = deletion
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mutation"></param>
		/// <returns></returns>
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
				Name = column.GetNameBytes(),
				Value = column.GetValueBytes(),
				Timestamp = column.GetTimestamp()
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public static ColumnOrSuperColumn CreateColumnOrSuperColumn(IFluentColumn column)
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
					Name = colY.NameBytes,
					Columns = new List<Column>(colY.Columns.Count)
				};

				foreach (var col in colY.Columns)
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

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnNames"></param>
		/// <returns></returns>
		public static SlicePredicate CreateSlicePredicate(List<byte[]> columnNames)
		{
			return new SlicePredicate {
				Column_names = columnNames
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="start"></param>
		/// <param name="finish"></param>
		/// <param name="reversed"></param>
		/// <param name="count"></param>
		/// <returns></returns>
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
