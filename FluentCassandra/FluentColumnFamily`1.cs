using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.Linq.Expressions;
using System.ComponentModel;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public abstract class FluentColumnFamily<T> : DynamicObject, IFluentColumnFamily, INotifyPropertyChanged, IEnumerable<FluentColumnPath>
		where T : IFluentColumn
	{
		public FluentColumnFamily(ColumnType columnType = ColumnType.Normal)
		{
			ColumnType = columnType;
		}

		public string Key { get; set; }
		public string Name { get; set; }
		public ColumnType ColumnType { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public IList<T> Columns { get; protected set; }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			IFluentColumn col = Columns.FirstOrDefault(c => c.Name == binder.Name);

			result = (col == null) ? null : col.GetValue();
			return col != null;
		}

		#region INotifyPropertyChanged Members

		public event PropertyChangedEventHandler PropertyChanged;

		protected void OnPropertyChanged(string propertyName)
		{
			if (PropertyChanged != null)
				PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
		}

		#endregion

		#region IEnumerable<FluentColumnPath<string>> Members

		public IEnumerator<FluentColumnPath> GetEnumerator()
		{
			foreach (var col in Columns)
				yield return col.GetPath();
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion
	}
}
