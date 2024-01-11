using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using IT_Core_SyncDB.Interface;

namespace IT_Core_SyncDB.Model
{
    public class DBHandle : IDB
    {
        public string _DBName { get; set; } = "";
        public string _ConnectionString { get; set; } = "";
        public SqlConnection _SqlConnection { get; set; } = new SqlConnection();

        public DBHandle(string ConnectionString, string DBName)
        {
            if (!string.IsNullOrEmpty(ConnectionString) && !string.IsNullOrEmpty(DBName))
            {
                _ConnectionString = ConnectionString;
                _DBName = DBName;
                _SqlConnection = new SqlConnection(_ConnectionString);
            }
        }

        public bool AlterSQLParameter(string SQL, Dictionary<string, object> Parameters)
        {
            int QueryNum = 0;
            if (_SqlConnection.State != ConnectionState.Open) _SqlConnection.Open();

            SqlCommand command = new SqlCommand(SQL, _SqlConnection);
            foreach (KeyValuePair<string, object> kvp in Parameters)
            {
                SqlParameter dp = command.CreateParameter();
                dp.ParameterName = kvp.Key;
                dp.Value = kvp.Value;
                command.Parameters.Add(dp);

                dp = new SqlParameter();
            }
            QueryNum = command.ExecuteNonQuery();
            _SqlConnection.Close();
            _SqlConnection.Dispose();
            return (QueryNum > 0);
        }

        public DataTable GetSQLParameter(string SQL, Dictionary<string, object> Parameters)
        {
            DataTable DT = new DataTable();

            if (!string.IsNullOrEmpty(SQL))
            {
                if (_SqlConnection.State != ConnectionState.Open) _SqlConnection.Open();
                lock (this)
                {
                    using (SqlCommand command = new SqlCommand(SQL, _SqlConnection))
                    {
                        command.CommandType = CommandType.Text;
                        command.CommandText = SQL;
                        command.CommandTimeout = 60;

                        foreach (KeyValuePair<string, object> kvp in Parameters)
                        {
                            SqlParameter dp = command.CreateParameter();
                            dp.ParameterName = kvp.Key;
                            dp.Value = kvp.Value;
                            command.Parameters.Add(dp);

                            dp = new SqlParameter();
                        }

                        DT = new DataTable();
                        SqlDataReader objReader = command.ExecuteReader();
                        DT.Load(objReader);
                        objReader.Close();

                        command.Dispose();
                    }
                }
            }
            return DT;
        }

        public void InsertBySqlBulk(SQLBulkParam _SQLBulkParam)
        {
            lock (this)
            {
                //SqlBulkCopy要多重設定，可用|隔開， ex: SqlBulkCopyOptions.KeepIdentity|SqlBulkCopyOptions.KeepNulls
                using (SqlBulkCopy sqlBC = new SqlBulkCopy(_ConnectionString, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.UseInternalTransaction))
                {
                    //設定一個批次量寫入多少筆資料
                    sqlBC.BatchSize = _SQLBulkParam.InbulkNum;
                    //設定逾時的秒數
                    sqlBC.BulkCopyTimeout = _SQLBulkParam.BulkCopyTimeoutSec;
                    //設定要寫入的資料庫
                    sqlBC.DestinationTableName = _SQLBulkParam.TableName;

                    foreach (var column in _SQLBulkParam.ColumnList)
                    {
                        //對應資料行
                        sqlBC.ColumnMappings.Add(column, column);
                    }
                    //開始寫入
                    //WriteToServer無回傳值
                    //errorHandle由exception抓取
                    sqlBC.WriteToServer(_SQLBulkParam.DTInsert);
                }
            }
        }
    }
}