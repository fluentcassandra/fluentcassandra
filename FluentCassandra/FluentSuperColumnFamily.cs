using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.Linq.Expressions;
using System.ComponentModel;

namespace FluentCassandra
{
	public class FluentSuperColumnFamily : FluentColumnFamily<FluentSuperColumn>
	{
		public FluentSuperColumnFamily()
			: base(ColumnType.Super)
		{
			Columns = new FluentSuperColumnList(this);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public override bool TrySetMember(SetMemberBinder binder, object value)
		{
			if (!(value is FluentSuperColumn))
				throw new ArgumentException("Value must be of type FluentSuperColumn<string>, because this column family is of type Super.", "value");

			var col = Columns.FirstOrDefault(c => c.Name == binder.Name);

			// if column doesn't exisit create it and add it to the columns
			if (col == null)
			{
				col = (FluentSuperColumn)value;
				col.Name = binder.Name;

				Columns.Add(col);
			}

			// notify that property has changed
			OnPropertyChanged(binder.Name);

			return true;
		}
	}
}
