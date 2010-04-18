using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.ComponentModel;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	public class FluentSuperColumn : FluentSuperColumn<string> { }

	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class FluentSuperColumn<T> : DynamicObject, IFluentColumn<T>, INotifyPropertyChanged, IEnumerable<FluentColumn<string>>
	{
		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumn()
		{
			Columns = new List<FluentColumn<string>>();
		}

		/// <summary>
		/// The super column name.
		/// </summary>
		public T Name { get; set; }

		internal byte[] NameBytes
		{
			get { return Name.GetBytes(); }
		}

		/// <summary>
		/// The columns in the super column.
		/// </summary>
		public IList<FluentColumn<string>> Columns { get; private set; }

		/// <summary>
		/// The columns in the super column.
		/// </summary>
		object IFluentColumn<T>.Value
		{
			get { return Columns; }
			set { throw new NotSupportedException("You need to use the Columns proprety for the Super Column type."); }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			FluentColumn<string> col = Columns.FirstOrDefault(c => c.Name == binder.Name);

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
