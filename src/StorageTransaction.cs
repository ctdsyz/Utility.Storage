using System;
using System.Data;
using System.Data.SqlClient;

namespace Utility.Storage.StorageHelper
{
    /// <summary>
    /// 事务处理. 若未提交事务或提交失败，在Dispose时会自动回滚
    /// </summary>
    public interface IStorageTransaction : IDisposable
    {
        void Complete();
    }

    internal class RealStorageTransaction : IStorageTransaction
    {
        internal RealStorageTransaction(IsolationLevel isolationLevel)
        {
            this.isolationLevel = isolationLevel;
        }

        internal TransactionConnection Connection
        {
            get { return this.connection; }
            set
            {
                if (value == null) throw new ArgumentNullException();

                this.connection = value;
                this.connection.Open();
                this.transaction = this.connection.BeginTransaction(this.isolationLevel);
            }
        }

        internal IDbTransaction Transaction
        {
            get { return this.transaction; }
        }

        internal bool Disposed { get; private set; }

        /// <summary>
        /// 提交事务
        /// </summary>
        public void Complete()
        {
            //if (this.connection == null) throw new InvalidOperationException("Connection is null");
            if (this.connection == null) return;

            this.transaction.Commit();
            this.committed = true;
        }

        /// <summary>
        /// 若事务失败，Dispose时会自动回滚
        /// </summary>
        public void Dispose()
        {
            this.Disposed = true;

            if (this.connection == null) return;

            try
            {
                if (this.transaction != null && !this.committed)
                {
                    this.transaction.Rollback();
                }
            }
            finally
            {
                this.connection.ForceDispose();
            }
        }

        private bool committed;

        private IsolationLevel isolationLevel;

        private TransactionConnection connection;

        private IDbTransaction transaction;
    }

    internal class NestedStorageTransaction : IStorageTransaction
    {
        public NestedStorageTransaction(RealStorageTransaction ambient)
        {
            if (ambient == null) throw new ArgumentNullException();
            if (ambient.Connection == null) throw new InvalidOperationException("No Connection");
            if (ambient.Connection.Transaction == null) throw new InvalidOperationException("No Transaction");
            if (!(ambient.Connection.Transaction is SqlTransaction)) throw new NotSupportedException("Sql Server Only");

            this.ambient = ambient;

            this.name = Convert.ToBase64String(Guid.NewGuid().ToByteArray());

            (this.ambient.Connection.Transaction as SqlTransaction).Save(this.name);
        }

        public void Complete()
        {
            this.committed = true;
        }

        public void Dispose()
        {
            if (this.disposed) return;

            if (!this.committed)
            {
                (this.ambient.Connection.Transaction as System.Data.SqlClient.SqlTransaction).Rollback(this.name);
            }

            this.disposed = true;
        }

        public string Name
        {
            get { return this.name; }
        }

        private string name;
        private bool committed;
        private bool disposed;
        private RealStorageTransaction ambient;

        private int totoal;
    }

    internal class EmptyStorageTransaction : IStorageTransaction
    {
        private EmptyStorageTransaction() { }

        public void Complete()
        {

        }

        public void Dispose()
        {

        }

        public readonly static IStorageTransaction Instance = new EmptyStorageTransaction();
    }

}
