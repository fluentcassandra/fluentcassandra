using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.Linq.Expressions;
using System.ComponentModel;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class FluentColumnFamily<CompareWith> : FluentRecord<IFluentColumn<CompareWith>>, IFluentColumnFamily<CompareWith>
		where CompareWith : CassandraType
	{
		private FluentColumnList<IFluentColumn<CompareWith>> _columns;
		private FluentColumnParent _self;

		public FluentColumnFamily(string key, string columnFamily)
		{
			Key = key;
			FamilyName = columnFamily;

			_self = new FluentColumnParent(this, null);
			_columns = new FluentColumnList<IFluentColumn<CompareWith>>(_self);

		}

		/// <summary>
		/// 
		/// </summary>
		public string Key { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public string FamilyName { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public ColumnType ColumnType { get { return ColumnType.Standard; } }

		/// <summary>
		/// 
		/// </summary>
		public override IList<IFluentColumn<CompareWith>> Columns
		{
			get { return _columns; }
		}

		/// <summary>
		/// Gets the path.
		/// </summary>
		/// <returns></returns>
		public FluentColumnPath GetPath()
		{
			return new FluentColumnPath(_self, null);
		}

		/// <summary>
		/// Gets the parent.
		/// </summary>
		/// <returns></returns>
		public FluentColumnParent GetSelf()
		{
			return _self;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		public override bool TryGetColumn(object name, out object result)
		{
			var col = Columns.FirstOrDefault(c => c.Name == name);

			result = (col == null) ? null : col.Value;
			return col != null;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public override bool TrySetColumn(object name, object value)
		{
			var col = Columns.FirstOrDefault(c => c.Name == name);
			var mutationType = MutationType.Changed;

			// if column doesn't exisit create it and add it to the columns
			if (col == null)
			{
				mutationType = MutationType.Added;

				col = new FluentColumn<CompareWith>();
				((FluentColumn<CompareWith>)col).Name = CassandraType.GetType<CompareWith>(col.Name);

				_columns.SupressChangeNotification = true;
				_columns.Add(col);
				_columns.SupressChangeNotification = false;
			}

			// set the column value
			col.Value = CassandraType.GetType<BytesType>(value);

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}
	}
}
