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
	/// <typeparam name="T"></typeparam>
	public class FluentSuperColumn : DynamicObject, IFluentColumn, INotifyPropertyChanged, IEnumerable<FluentColumn>
	{
		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumn()
		{
			Columns = new FluentColumnList(Family, this);
		}

		/// <summary>
		/// The super column name.
		/// </summary>
		public string Name { get; set; }

		internal byte[] NameBytes
		{
			get { return Name.GetBytes(); }
		}

		/// <summary>
		/// The columns in the super column.
		/// </summary>
		public IList<FluentColumn> Columns { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		object IFluentColumn.GetValue()
		{
			return Columns;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		T IFluentColumn.GetValue<T>()
		{
			throw new NotSupportedException("You need to use the Columns proprety for the Super Column type.");
		}

		/// <summary>
		/// The column value.
		/// </summary>
		void IFluentColumn.SetValue(object obj)
		{
			throw new NotSupportedException("You need to use the Columns proprety for the Super Column type.");
		}

		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumnFamily Family
		{
			get;
			internal set;
		}

		/// <summary>
		/// 
		/// </summary>
		IFluentColumnFamily IFluentColumn.Family
		{
			get { return Family; }
		}

		/// <summary>
		/// 
		/// </summary>
		FluentSuperColumn IFluentColumn.SuperColumn
		{
			get { return null; }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public FluentColumnPath GetPath()
		{
			return new FluentColumnPath(Family, this, null);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public FluentColumnParent GetParent()
		{
			return new FluentColumnParent(Family, this);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			FluentColumn col = Columns.FirstOrDefault(c => c.Name == binder.Name);

			result = (col == null) ? null : col.GetValue();
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
			FluentColumn col = Columns.FirstOrDefault(c => c.Name == binder.Name);

			// if column doesn't exisit create it and add it to the columns
			if (col == null)
			{
				col = new FluentColumn {
					Name = binder.Name
				};

				Columns.Add(col);
			}

			// set the column value
			col.SetValue(value);

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

		#region IEnumerable<FluentColumn> Members

		public IEnumerator<FluentColumn> GetEnumerator()
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
