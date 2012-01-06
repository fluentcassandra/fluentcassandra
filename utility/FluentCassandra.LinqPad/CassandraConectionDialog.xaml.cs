using System;
using System.Windows;

namespace FluentCassandra.LinqPad
{
    public partial class CassandraConectionDialog : Window
    {
        private CassandraConnectionInfo _connInfo;

        public CassandraConectionDialog(CassandraConnectionInfo conn)
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
