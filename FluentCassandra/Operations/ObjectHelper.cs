using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	internal static class ObjectHelper
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
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

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		public static FluentColumn<CompareWith> ConvertColumnToFluentColumn<CompareWith>(Column col)
			where CompareWith : CassandraType
		{
			return new FluentColumn<CompareWith> {
				Name = CassandraType.GetType<CompareWith>(col.Name),
				Value = col.Value,
				Timestamp = new DateTimeOffset(col.Timestamp, TimeSpan.Zero)
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		public static FluentSuperColumn<CompareWith, CompareSubcolumnWith> ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(SuperColumn col)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superCol = new FluentSuperColumn<CompareWith, CompareSubcolumnWith> {
				Name = CassandraType.GetType<CompareWith>(col.Name)
			};

			foreach (var xcol in col.Columns)
				superCol.Columns.Add(ConvertColumnToFluentColumn<CompareSubcolumnWith>(xcol));

			return superCol;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mutation"></param>
		/// <returns></returns>
		public static Mutation CreateDeletedColumnMutation(IEnumerable<FluentMutation> mutation)
		{
			var columnNames = mutation.Select(m => m.Column.Name).ToList();

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
			var superColumn = mutation.Select(m => m.Column.GetParent().SuperColumn.Name).FirstOrDefault();
			var columnNames = mutation.Select(m => m.Column.Name).ToList();

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

		public static Column CreateColumn(IFluentColumn column)
		{
			return new Column {
				Name = column.Name,
				Value = column.Value,
				Timestamp = column.Timestamp.UtcTicks
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
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
					Name = colY.Name,
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

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnNames"></param>
		/// <returns></returns>
		public static SlicePredicate CreateSlicePredicate(List<CassandraType> columnNames)
		{
			return new SlicePredicate {
				Column_names = columnNames.Cast<byte[]>().ToList()
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
