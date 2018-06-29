using System;
using System.Data;

namespace Utility.Storage.StorageHelper
{
    class TransactionConnection : IDbConnection
    {
        public TransactionConnection(Storage storage, IDbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException();

            this.connection = connection;
            this.Storage = storage;
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            return (this.Transaction = this.connection.BeginTransaction(il));
        }

        public IDbTransaction BeginTransaction()
        {
            return (this.Transaction = this.connection.BeginTransaction());
        }

        public void ChangeDatabase(string databaseName)
        {
            this.connection.ChangeDatabase(databaseName);
        }

        /// <summary>
        /// 若已开始事务则只能调用ForceClose关闭
        /// </summary>
        public void Close()
        {
            if (this.Transaction != null) return;

            this.connection.Close();
        }

        public IDbCommand CreateCommand()
        {
            return this.connection.CreateCommand();
        }

        public string Database
        {
            get { return this.connection.Database; }
        }

        public void Open()
        {
            if (this.State == ConnectionState.Closed)
            {
                this.connection.Open();
            }
        }

        public ConnectionState State
        {
            get { return this.connection.State; }
        }

        /// <summary>
        /// 若已开始事务则只能调用ForceDispose释放资源
        /// </summary>
        public void Dispose()
        {
            if (this.Transaction != null) return;

            this.connection.Dispose();
        }

        public void ForceClose()
        {
            this.connection.Close();
        }

        public void ForceDispose()
        {
            this.connection.Dispose();
        }

        public string ConnectionString
        {
            get
            {
                return this.connection.ConnectionString;
            }
            set
            {
                this.connection.ConnectionString = value;
            }
        }

        public int ConnectionTimeout
        {
            get { return this.connection.ConnectionTimeout; }
        }

        public Storage Storage { get; private set; }

        public IDbTransaction Transaction { get; private set; }

        private IDbConnection connection;
        
    }
}
