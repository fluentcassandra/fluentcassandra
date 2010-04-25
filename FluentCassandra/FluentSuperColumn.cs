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
	public class FluentSuperColumn : FluentRecord<FluentColumn>, IFluentSuperColumn, IFluentColumn
	{
		private FluentSuperColumnFamily _family;
		private FluentColumnList<FluentColumn> _columns;

		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumn()
		{
			_columns = new FluentColumnList<FluentColumn>(GetParent());
		}

		public string Name { get; set; }

		internal byte[] NameBytes
		{
			get { return Name.GetBytes(); }
		}

		/// <summary>
		/// The columns in the super column.
		/// </summary>
		public override IList<FluentColumn> Columns
		{
			get { return _columns; }
		}

		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumnFamily Family
		{
			get { return _family; }
			internal set
			{
				_family = value;
			
				var parent = GetParent();
				_columns.Parent = parent;
				foreach (var col in Columns)
					((IFluentColumn)col).SetParent(parent);
			}
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

		#region IFluentColumn Members

		object IFluentColumn.GetValue()
		{
			return Columns;
		}

		object IFluentColumn.GetValue(Type type)
		{
			throw new NotSupportedException("You need to use the Columns proprety for the Super Column type.");
		}

		T IFluentColumn.GetValue<T>()
		{
			throw new NotSupportedException("You need to use the Columns proprety for the Super Column type.");
		}

		void IFluentColumn.SetValue(object obj)
		{
			throw new NotSupportedException("You need to use the Columns proprety for the Super Column type.");
		}

		IFluentColumnFamily IFluentColumn.Family
		{
			get { return Family; }
		}

		IFluentSuperColumn IFluentColumn.SuperColumn
		{
			get { return null; }
		}

		void IFluentColumn.SetParent(FluentColumnParent parent)
		{
			if (!(parent.ColumnFamily is FluentSuperColumnFamily))
				throw new CassandraException("Only a super column family can have a child that is a super column.");

			Family = (FluentSuperColumnFamily)parent.ColumnFamily;
		}

		#endregion
	}
}
