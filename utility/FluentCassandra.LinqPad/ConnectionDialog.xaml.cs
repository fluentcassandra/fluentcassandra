using System;
using System.Linq;
using System.Windows;

namespace FluentCassandra.LinqPad
{
	/// <summary>
	/// Interaction logic for ConnectionDialog.xaml
	/// </summary>
	public partial class ConnectionDialog : Window
	{
		private CassandraConnectionInfo _connInfo;

		public ConnectionDialog(CassandraConnectionInfo conn)
		{
			if (conn == null)
				throw new ArgumentNullException("conn", "conn is null.");

			InitializeComponent();

			DataContext = _connInfo = conn;
			PasswordValue.Password = conn.Password;
		}
		private void OkButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}

		private void PasswordValue_PasswordChanged(object sender, RoutedEventArgs e)
		{
			_connInfo.Password = PasswordValue.Password;
		}
	}
}