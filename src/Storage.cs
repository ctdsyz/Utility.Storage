/*
 License: http://www.apache.org/licenses/LICENSE-2.0 
 Home page: https://github.com/ctdsyz/Utility.Storage

 Note: 您拥有在本地自由使用及修改本源代码的权限，但如果本源代码及其修改代码在被编译时被使用了代码混淆功能，则视为您主动放弃这个权限
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Utility.Storage.StorageHelper.Common;

namespace Utility.Storage.StorageHelper
{
    /// <summary>
    /// 数据库操作类. 查询时直接返回实体， 目前内部使用Dapper
    /// </summary>
    public class Storage
    {
        /// <summary>
        /// 数据库操作类.
        /// </summary>
        public Storage(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("connectionString is invalid");
            }

            this.connectionString = connectionString;
        }

        /// <summary>
        /// 根据列名分割出若干个实体，经convert转换后输出.
        /// e.g. 列名: "A1,A2,A3,B1,B2,B3,C1,C2"中"A1,A2,A3"对应T1, "B1,B2,B3"对应T2,"C1,C2"对应T3 => splitOn = "B1,C1"
        /// </summary>
        /// <param name="convert">根据各类型转换出所需结果</param>
        /// <param name="splitOn">用以分割的字段名集合，以英文逗号分隔</param>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>经convert转换后的结果集</returns>
        public TResult[] Query<T1, T2, TResult>(Func<T1, T2, TResult> convert, string splitOn, string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            if (convert == null)
            {
                throw new ArgumentNullException("convert");
            }

            if (string.IsNullOrWhiteSpace(splitOn))
            {
                throw new ArgumentException("splitOn");
            }

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Query[T1, T2]"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Query<T1, T2, TResult>(sql, convert, param.ToDapperParameters(), conn.Transaction, buffered, splitOn, GetTimeout(timeout), commandType).ToArray();
            }
        }

        /// <summary>
        /// 根据列名分割出若干个实体，经convert转换后输出.
        /// e.g. 列名: "A1,A2,A3,B1,B2,B3,C1,C2"中"A1,A2,A3"对应T1, "B1,B2,B3"对应T2,"C1,C2"对应T3 => splitOn = "B1,C1"
        /// </summary>
        /// <param name="convert">根据各类型转换出所需结果</param>
        /// <param name="splitOn">用以分割的字段名集合，以英文逗号分隔</param>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>经convert转换后的结果集</returns>
        public TResult[] Query<T1, T2, T3, TResult>(Func<T1, T2, T3, TResult> convert, string splitOn, string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            if (convert == null)
            {
                throw new ArgumentNullException("convert");
            }

            if (string.IsNullOrWhiteSpace(splitOn))
            {
                throw new ArgumentException("splitOn");
            }

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Query[T1, T2, T3]"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Query<T1, T2, T3, TResult>(sql, convert, param.ToDapperParameters(), conn.Transaction, buffered, splitOn, GetTimeout(timeout), commandType).ToArray();
            }
        }

        /// <summary>
        /// 根据列名分割出若干个实体，经convert转换后输出.
        /// e.g. 列名: "A1,A2,A3,B1,B2,B3,C1,C2"中"A1,A2,A3"对应T1, "B1,B2,B3"对应T2,"C1,C2"对应T3 => splitOn = "B1,C1"
        /// </summary>
        /// <param name="convert">根据各类型转换出所需结果</param>
        /// <param name="splitOn">用以分割的字段名集合，以英文逗号分隔</param>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>经convert转换后的结果集</returns>
        public TResult[] Query<T1, T2, T3, T4, TResult>(Func<T1, T2, T3, T4, TResult> convert, string splitOn, string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            if (convert == null)
            {
                throw new ArgumentNullException("convert");
            }

            if (string.IsNullOrWhiteSpace(splitOn))
            {
                throw new ArgumentException("splitOn");
            }

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Query[T1, T2, T3, T4]"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Query<T1, T2, T3, T4, TResult>(sql, convert, param.ToDapperParameters(), conn.Transaction, buffered, splitOn, GetTimeout(timeout), commandType).ToArray();
            }
        }

        /// <summary>
        /// 根据列名分割出若干个实体，经convert转换后输出.
        /// e.g. 列名: "A1,A2,A3,B1,B2,B3,C1,C2"中"A1,A2,A3"对应T1, "B1,B2,B3"对应T2,"C1,C2"对应T3 => splitOn = "B1,C1"
        /// </summary>
        /// <param name="convert">根据各类型转换出所需结果</param>
        /// <param name="splitOn">用以分割的字段名集合，以英文逗号分隔</param>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>经convert转换后的结果集</returns>
        public TResult[] Query<T1, T2, T3, T4, T5, TResult>(Func<T1, T2, T3, T4, T5, TResult> convert, string splitOn, string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            if (convert == null)
            {
                throw new ArgumentNullException("convert");
            }

            if (string.IsNullOrWhiteSpace(splitOn))
            {
                throw new ArgumentException("splitOn");
            }

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Query[T1, T2, T3, T4, T5]"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Query<T1, T2, T3, T4, T5, TResult>(sql, convert, param.ToDapperParameters(), conn.Transaction, buffered, splitOn, GetTimeout(timeout), commandType).ToArray();
            }
        }

        /// <summary>
        /// 根据列名分割出若干个实体，经convert转换后输出.
        /// e.g. 列名: "A1,A2,A3,B1,B2,B3,C1,C2"中"A1,A2,A3"对应T1, "B1,B2,B3"对应T2,"C1,C2"对应T3 => splitOn = "B1,C1"
        /// </summary>
        /// <param name="convert">根据各类型转换出所需结果</param>
        /// <param name="splitOn">用以分割的字段名集合，以英文逗号分隔</param>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>经convert转换后的结果集</returns>
        public TResult[] Query<T1, T2, T3, T4, T5, T6, TResult>(Func<T1, T2, T3, T4, T5, T6, TResult> convert, string splitOn, string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            if (convert == null)
            {
                throw new ArgumentNullException("convert");
            }

            if (string.IsNullOrWhiteSpace(splitOn))
            {
                throw new ArgumentException("splitOn");
            }

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Query[T1, T2, T3, T4, T5, T6]"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Query<T1, T2, T3, T4, T5, T6, TResult>(sql, convert, param.ToDapperParameters(), conn.Transaction, buffered, splitOn, GetTimeout(timeout), commandType).ToArray();
            }
        }

        /// <summary>
        /// 根据列名分割出若干个实体，经convert转换后输出.
        /// e.g. 列名: "A1,A2,A3,B1,B2,B3,C1,C2"中"A1,A2,A3"对应T1, "B1,B2,B3"对应T2,"C1,C2"对应T3 => splitOn = "B1,C1"
        /// </summary>
        /// <param name="convert">根据各类型转换出所需结果</param>
        /// <param name="splitOn">用以分割的字段名集合，以英文逗号分隔</param>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>经convert转换后的结果集</returns>
        public TResult[] Query<T1, T2, T3, T4, T5, T6, T7, TResult>(Func<T1, T2, T3, T4, T5, T6, T7, TResult> convert, string splitOn, string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            if (convert == null)
            {
                throw new ArgumentNullException("convert");
            }

            if (string.IsNullOrWhiteSpace(splitOn))
            {
                throw new ArgumentException("splitOn");
            }

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Query[T1, T2, T3, T4, T5, T6]"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Query<T1, T2, T3, T4, T5, T6, T7, TResult>(sql, convert, param.ToDapperParameters(), conn.Transaction, buffered, splitOn, GetTimeout(timeout), commandType).ToArray();
            }
        }

        /// <summary>
        /// 查询并返回实体集合
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>实体集合</returns>
        public T[] Query<T>(string sql, Parameters param, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Query"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Query<T>(sql, param?.Result, conn.Transaction, buffered, GetTimeout(timeout), commandType).ToArray();
            }
        }

        /// <summary>
        /// 查询并返回实体集合
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>实体集合</returns>
        public T[] Query<T>(string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Query"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Query<T>(sql, param.ToDapperParameters(), conn.Transaction, buffered, GetTimeout(timeout), commandType).ToArray();
            }
        }


        /// <summary>
        /// 查询并返回实体集合
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="sql">参数化的SQL语句</param>
        /// <returns>实体集合</returns>
        public T[] Query<T>(string sql)
        {
            return Query<T>(sql, (Parameters)null);
        }


        /// <summary>
        /// 多结果集查询并根据转换器返回结果
        /// </summary>
        /// <typeparam name="T">结果类型</typeparam>
        /// <param name="convert">转换器, 可从其参数中获取每一结果集</param>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="commandType">命令类型</param>
        /// <returns>结果</returns>
        public T QueryMultiple<T>(Func<ResultsReader, T> convert, string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null)
        {
            if (convert == null)
            {
                throw new ArgumentNullException("convert");
            }

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("QueryMultiple"))
            using (var conn = GetConnection())
            {
                conn.Open();
                watch.Next("query...");
                using (var m = conn.QueryMultiple(sql, param.ToDapperParameters(), conn.Transaction, GetTimeout(timeout), commandType))
                {
                    watch.Next("convert");
                    return convert(new ResultsReader(m));
                }
            }
        }

        /// <summary>
        /// 多结果集查询并根据转换器返回结果
        /// </summary>
        /// <typeparam name="T">结果类型</typeparam>
        /// <param name="convert">转换器, 可从其参数中获取每一结果集</param>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="commandType">命令类型</param>
        /// <returns>结果</returns>
        public T QueryMultiple<T>(Func<ResultsReader, T> convert, string sql, Parameters param, int? timeout = null, CommandType? commandType = null)
        {
            if (convert == null)
            {
                throw new ArgumentNullException("convert");
            }

            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("QueryMultiple"))
            using (var conn = GetConnection())
            {
                conn.Open();
                watch.Next("query...");
                using (var m = conn.QueryMultiple(sql, param?.Result, conn.Transaction, GetTimeout(timeout), commandType))
                {
                    watch.Next("convert");
                    return convert(new ResultsReader(m));
                }
            }
        }

        /// <summary>
        /// 多结果集查询并根据转换器返回结果
        /// </summary>
        /// <typeparam name="T">结果类型</typeparam>
        /// <param name="convert">转换器, 可从其参数中获取每一结果集</param>
        /// <param name="sql">参数化的SQL语句</param>
        /// <returns>结果</returns>
        public T QueryMultiple<T>(Func<ResultsReader, T> convert, string sql)
        {
            return QueryMultiple<T>(convert, sql, (Parameters)null);
        }

        /// <summary>
        /// 查询并返回单个结果。若查询无结果，返回null或值类型的默认值
        /// </summary>
        /// <typeparam name="T">结果类型</typeparam>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>结果</returns>
        public T Single<T>(string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            return this.Query<T>(sql, param, GetTimeout(timeout), commandType, buffered).FirstOrDefault();
        }

        /// <summary>
        /// 查询并返回单个结果。若查询无结果，返回null或值类型的默认值
        /// </summary>
        /// <typeparam name="T">结果类型</typeparam>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>结果</returns>
        public T Single<T>(string sql, Parameters param, int? timeout = null, CommandType? commandType = null, bool buffered = true)
        {
            return this.Query<T>(sql, param, GetTimeout(timeout), commandType, buffered).FirstOrDefault();
        }

        /// <summary>
        /// 查询并返回单个结果。若查询无结果，返回null或值类型的默认值
        /// </summary>
        /// <typeparam name="T">结果类型</typeparam>
        /// <param name="sql">参数化的SQL语句</param>
        /// <returns>结果</returns>
        public T Single<T>(string sql)
        {
            return this.Single<T>(sql, (Parameters)null);
        }


        /// <summary>
        /// 返回单个值类型结果
        /// </summary>
        /// <typeparam name="T">结果类型</typeparam>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>结果</returns>
        public T Value<T>(string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null) where T : struct
        {
            return this.ExecuteScalar<T>(sql, param, GetTimeout(timeout), commandType);
        }

        /// <summary>
        /// 返回单个字符串结果
        /// </summary>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>结果</returns>
        public string Value(string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null)
        {
            return this.ExecuteScalar<string>(sql, param, GetTimeout(timeout), commandType);
        }

        /// <summary>
        /// 返回bool结果
        /// </summary>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="buffered">是否缓存</param>
        /// <returns>结果</returns>
        public bool Bool(string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null)
        {
            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("ExecuteScalar"))
            using (var conn = GetConnection())
            {
                conn.Open();
                var o = conn.ExecuteScalar(sql, param.ToDapperParameters(), conn.Transaction, GetTimeout(timeout), commandType);
                return Convert.ToBoolean(o);
            }
        }

        /// <summary>
        /// 执行SQL语句或存储过程，返回影响行数
        /// </summary>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <returns>影响行数</returns>
        public int Execute(string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null)
        {
            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Execute"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Execute(sql, param.ToDapperParameters(), conn.Transaction, GetTimeout(timeout), commandType);
            }
        }

        /// <summary>
        /// 执行SQL语句或存储过程，返回影响行数
        /// </summary>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <returns>影响行数</returns>
        public int Execute(string sql, Parameters param, int? timeout = null, CommandType? commandType = null)
        {
            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("Execute"))
            using (var conn = GetConnection())
            {
                conn.Open();
                return conn.Execute(sql, param?.Result, conn.Transaction, GetTimeout(timeout), commandType);
            }
        }

        /// <summary>
        /// 执行SQL语句，返回影响行数
        /// </summary>
        /// <param name="sql">参数化的SQL语句</param>
        /// <returns>影响行数</returns>
        public int Execute(string sql)
        {
            return Execute(sql, (Parameters)null);
        }


        /// <summary>
        /// 执行SQL语句或存储过程，select单个值返回
        /// </summary>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <returns>select单个值返回</returns>
        public T ExecuteScalar<T>(string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null)
        {
            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("ExecuteScalar"))
            using (var conn = GetConnection())
            {
                conn.Open();
                var o = conn.ExecuteScalar(sql, param.ToDapperParameters(), conn.Transaction, GetTimeout(timeout), commandType);
                return o == null || o == DBNull.Value ? default(T) : (T)o;
            }
        }

        /// <summary>
        /// 执行SQL语句或存储过程，select单个值返回
        /// </summary>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <returns>select单个值返回</returns>
        public T ExecuteScalar<T>(string sql, Parameters param, int? timeout = null, CommandType? commandType = null)
        {
            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("sql should not be null", "sql");
            }

            using (var watch = new ElapsedWatch("ExecuteScalar"))
            using (var conn = GetConnection())
            {
                conn.Open();
                var o = conn.ExecuteScalar(sql, param?.Result, conn.Transaction, GetTimeout(timeout), commandType);
                return o == null || o == DBNull.Value ? default(T) : (T)o;
            }
        }

        /// <summary>
        /// 执行SQL语句，select单个值返回
        /// </summary>
        /// <param name="sql">参数化的SQL语句</param>
        /// <returns>select单个值返回</returns>
        public T ExecuteScalar<T>(string sql)
        {
            return this.ExecuteScalar<T>(sql, (Parameters)null);
        }

        /// <summary>
        /// 执行SQL语句或存储过程，select单个int32值返回
        /// </summary>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <returns>select单个int32值返回</returns>
        public int ExecuteScalar(string sql, Parameters param, int? timeout = null, CommandType? commandType = null)
        {
            return this.ExecuteScalar<int>(sql, param, timeout, commandType);
        }

        /// <summary>
        /// 执行SQL语句或存储过程，select单个int32值返回
        /// </summary>
        /// <param name="sql">参数化的SQL语句或存储过程</param>
        /// <param name="param">参数</param>
        /// <param name="timeout">超时时间，单位为秒</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <returns>select单个int32值返回</returns>
        public int ExecuteScalar(string sql, Func<Parameters, Parameters> param = null, int? timeout = null, CommandType? commandType = null)
        {
            return this.ExecuteScalar<int>(sql, param, timeout, commandType);
        }

        /// <summary>
        /// 执行SQL语句，select单个int32值返回
        /// </summary>
        /// <param name="sql">参数化的SQL语句</param>
        /// <returns>select单个int32值返回</returns>
        public int ExecuteScalar(string sql)
        {
            return this.ExecuteScalar(sql, (Parameters)null);
        }


        /// <summary>
        /// 开始事务. 请务必使用using, 如using(var t = Storage.NewTransaction()){...}
        /// 限同一线程及同一数据库及使用本类的方法进行的查询和操作. 若嵌套使用则整体视为一个事务.
        /// </summary>
        public static IStorageTransaction NewTransaction(IsolationLevel isolationLevel)
        {
            return NewTransaction(false, isolationLevel);
        }

        /// <summary>
        /// 开始事务. 请务必使用using, 如using(var t = Storage.NewTransaction()){...}
        /// 限同一线程及同一数据库及使用本类的方法进行的查询和操作. 若嵌套使用则整体视为一个事务.
        /// innerNestedTranRollbackEnabled：作为外层事务的内部嵌套事务回滚是否有效，前提是数据库为Sql Server且外部事物至少访问过一次数据库，否则抛异常
        /// </summary>
        public static IStorageTransaction NewTransaction(bool innerNestedTranRollbackEnabled, IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            if (currentThreadTransaction != null && !currentThreadTransaction.Disposed)
            {
                if (innerNestedTranRollbackEnabled)
                {
                    if (currentThreadTransaction.Connection == null || currentThreadTransaction.Connection.Transaction == null)
                    {
                        throw new InvalidOperationException("outer transaction should visit database once at least");
                    }

                    if (!(currentThreadTransaction.Connection.Transaction is SqlTransaction))
                    {
                        throw new NotSupportedException("SQL Server Only");
                    }

                    return new NestedStorageTransaction(currentThreadTransaction);
                }

                return EmptyStorageTransaction.Instance;
            }

            return (currentThreadTransaction = new RealStorageTransaction(isolationLevel));
        }

        /// <summary>
        /// 开始事务. 请务必使用using, 如using(var t = Storage.NewTransaction()){...}
        /// 限同一线程及同一数据库及使用本类的方法进行的查询和操作. 若嵌套使用则整体视为一个事务.
        /// </summary>
        public static IStorageTransaction NewTransaction()
        {
            return NewTransaction(IsolationLevel.ReadCommitted);
        }

        /// <summary>
        /// 创建参数
        /// </summary>
        public static Parameters CreateParameters()
        {
            return new Parameters();
        }

        /// <summary>
        /// 默认超时时间为30秒
        /// </summary>
        public int? DefaultTimeout
        {
            get { return this.defaultTimeout; }
            set { this.defaultTimeout = value; }
        }

        /// <summary>
        /// 上次查询或操作时间
        /// </summary>
        public ElapsedInfo LastElapsedInfo
        {
            get
            {
                return ElapsedWatch.LastElapsed ?? new ElapsedInfo();
            }
        }

        /// <summary>
        /// System.Data.IDbConnection提供器，参数为链接字符串。默认使用System.Data.SqlClient.SqlConnection
        /// </summary>
        public Func<string, IDbConnection> ConnectionProvider
        {
            get { return connectionProvider; }
            set
            {
                if (value == null) throw new ArgumentNullException();
                connectionProvider = value;
            }
        }

        private IDbConnection GetConnectionProvider(string connectionString)
        {
            return ConnectionProvider.Invoke(connectionString);
        }

        private int? GetTimeout(int? timeout)
        {
            return timeout.HasValue ? timeout : this.DefaultTimeout;
        }

        private TransactionConnection GetConnection()
        {
            if (currentThreadTransaction != null && currentThreadTransaction.Disposed) currentThreadTransaction = null;

            if (currentThreadTransaction == null) return new TransactionConnection(this, GetConnectionProvider(this.connectionString));

            if (currentThreadTransaction.Connection == null)
            {
                currentThreadTransaction.Connection = new TransactionConnection(this, GetConnectionProvider(this.connectionString));
            }
            else
            {
                if (currentThreadTransaction.Connection.Storage != this)
                {
                    return new TransactionConnection(this, GetConnectionProvider(this.connectionString));
                }
            }

            return currentThreadTransaction.Connection;
        }

        private Func<string, IDbConnection> connectionProvider = conn => new SqlConnection(conn);

        private int? defaultTimeout = 30;

        private string connectionString;

        [ThreadStatic]
        private static RealStorageTransaction currentThreadTransaction;
    }


}
