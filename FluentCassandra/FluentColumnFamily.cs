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
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public class FluentColumnFamily : DynamicObject, INotifyPropertyChanged, IEnumerable<FluentColumn<string>>
	{
		public FluentColumnFamily()
		{
			Columns = new List<FluentColumn<string>>();
		}

		public string Key { get; set; }
		public string ColumnFamily { get; set; }

		public IList<FluentColumn<string>> Columns { get; private set; }

		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			FluentColumn<string> col = Columns.FirstOrDefault(c => c.Name == binder.Name);

			result = (col == null) ? null : col.Value;
			return col != null;
		}

		public override bool TrySetMember(SetMemberBinder binder, object value)
		{
			FluentColumn<string> col = Columns.FirstOrDefault(c => c.Name == binder.Name);

			// if column doesn't exisit create it and add it to the columns
			if (col == null)
			{
				col = new FluentColumn<string> {
					Name = binder.Name
				};

				Columns.Add(col);
			}

			// set the column value
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

		#region IEnumerable<FluentColumn<string>> Members

		public IEnumerator<FluentColumn<string>> GetEnumerator()
		{
			return Columns.GetEnumerator();
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
