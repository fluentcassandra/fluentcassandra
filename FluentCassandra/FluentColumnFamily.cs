using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.Linq.Expressions;
using System.ComponentModel;
using FluentCassandra.Configuration;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public class FluentColumnFamily : DynamicObject, INotifyPropertyChanged, IEnumerable<FluentColumnPath<string>>
	{
		public FluentColumnFamily(ColumnType columnType = ColumnType.Normal)
		{
			ColumnType = columnType;
			Columns = new List<IFluentColumn<string>>();
		}

		public string Key { get; set; }
		public string ColumnFamily { get; set; }
		public ColumnType ColumnType { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public IList<IFluentColumn<string>> Columns { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			IFluentColumn<string> col = Columns.FirstOrDefault(c => c.Name == binder.Name);

			result = (col == null) ? null : col.Value;
			return col != null;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public override bool TrySetMember(SetMemberBinder binder, object value)
		{
			if (ColumnType == ColumnType.Super && !(value is FluentSuperColumn<string>))
				throw new ArgumentException("Value must be of type FluentSuperColumn<string>, because this column family is of type Super.", "value");

			IFluentColumn<string> col = Columns.FirstOrDefault(c => c.Name == binder.Name);

			// if column doesn't exisit create it and add it to the columns
			if (col == null)
			{
				if (value is FluentSuperColumn<string>)
				{
					col = (FluentSuperColumn<string>)value;
					col.Name = binder.Name;
				}
				else
				{
					col = new FluentColumn<string> {
						Name = binder.Name
					};
				}

				Columns.Add(col);
			}

			// set the column value
			if (!(value is FluentSuperColumn<string>))
				col.Value = value;

			// notify that property has changed
			OnPropertyChanged(binder.Name);

			return true;
		}

		#region INotifyPropertyChanged Members

		public event PropertyChangedEventHandler PropertyChanged;

		private void OnPropertyChanged(string propertyName)
		{
			if (PropertyChanged != null)
				PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
		}

		#endregion

		#region IEnumerable<FluentColumnPath<string>> Members

		public IEnumerator<FluentColumnPath<string>> GetEnumerator()
		{
			foreach (var col in Columns)
			{
				if (col is FluentSuperColumn<string>)
				{
					var superCol = (FluentSuperColumn<string>)col;
					foreach (var col2 in superCol)
						yield return new FluentColumnPath<string>(this, superCol, col2);
				}
				else if (col is FluentColumn<string>)
				{
					yield return new FluentColumnPath<string>(this, null, (FluentColumn<string>)col);
				}
			}
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return Columns.GetEnumerator();
		}

		#endregion
	}
}
