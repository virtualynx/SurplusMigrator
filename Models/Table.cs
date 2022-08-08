using Microsoft.Data.SqlClient;
using Npgsql;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text.Json;

namespace SurplusMigrator.Models
{
    class Table
    {
        public DbConnection_ connection;
        public string tableName;
        public string[] columns;
        public string[] ids;
        public int batchSize;
        private long fetchDataCount = -1;
        private int fetchBatchCounter = 1;
        private int fetchBatchMax = -1;

        public Table() { }

        public Table(DbConnection_ connection, string tableName, string[] columns, int batchSize, string[] ids = null) {
            this.connection = connection;
            this.tableName = tableName;
            this.columns = columns;
            this.batchSize = batchSize;
            this.ids = ids;
        }

        public List<RowData<string, object>> getDatas() {
            List<RowData<string, object>> result = new List<RowData<string, object>> ();

            //check if all batch already fetched
            if(fetchBatchMax != -1 && fetchBatchCounter > fetchBatchMax) {
                fetchBatchCounter = 1;
                return result;
            }

            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                SqlConnection conn = (SqlConnection)connection.GetDbConnection();

                SqlCommand command = null;
                SqlDataReader dataReader = null;

                //get data count for first time open
                if(fetchDataCount == -1) {
                    command = new SqlCommand(
                        "SELECT COUNT(*) FROM " + connection.GetDbLoginInfo().schema + "." + tableName
                        , conn
                    );
                    dataReader = command.ExecuteReader();
                    dataReader.Read();
                    fetchDataCount = Convert.ToInt64(dataReader.GetValue(0));

                    dataReader.Close();
                    command.Dispose();

                    decimal d = Convert.ToDecimal(fetchDataCount) / Convert.ToDecimal(batchSize);
                    fetchBatchMax = (int)Math.Ceiling(d);
                }

                string sqlString = @"SELECT [selected_columns] 
                    FROM    ( SELECT    ROW_NUMBER() OVER ( ORDER BY [over_orderby] ) AS RowNum, *
                                FROM      [tablename]
                            ) AS RowConstrainedResult
                    WHERE   RowNum >= [offset_start]
                        AND RowNum <= [offset_end]
                    ORDER BY RowNum";

                sqlString = sqlString.Replace("[selected_columns]", String.Join(',', columns));
                string over_orderby = "(select null)";
                if(ids != null) {
                    over_orderby = String.Join(',', ids);
                }
                sqlString = sqlString.Replace("[over_orderby]", over_orderby);
                sqlString = sqlString.Replace("[tablename]", connection.GetDbLoginInfo().schema + "." + tableName);
                sqlString = sqlString.Replace("[offset_start]", (((fetchBatchCounter - 1) * batchSize) + 1).ToString());
                sqlString = sqlString.Replace("[offset_end]", (((fetchBatchCounter - 1) * batchSize) + batchSize).ToString());

                command = new SqlCommand(sqlString, conn);
                dataReader = command.ExecuteReader();

                while(dataReader.Read()) {
                    RowData<string, object> rowData = new RowData<string, object>();
                    for(int a = 0; a < columns.Length; a++) {
                        var value = dataReader.GetValue(a);
                        rowData.Add(columns[a], value.GetType() == typeof(System.DBNull) ? null : value);
                    }
                    result.Add(rowData);
                }

                dataReader.Close();
                command.Dispose();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                //NpgsqlConnection conn = (NpgsqlConnection)sourceConn.GetDbConnection();
                throw new System.NotImplementedException();
            }

            if(result.Count > 0) {
                fetchBatchCounter++;
            }

            return result;
        }

        public List<DbInsertError> insertData(List<RowData<string, object>> inputs) {
            List<DbInsertError> errors = new List<DbInsertError>();

            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                throw new System.NotImplementedException();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                NpgsqlConnection conn = (NpgsqlConnection)connection.GetDbConnection();

                NpgsqlCommand command = null;

                string sql = "INSERT INTO \"" + connection.GetDbLoginInfo().schema + "\"." + tableName + "(\"" + String.Join("\",\"", columns) + "\") VALUES ";
                List<string> sqlParams = new List<string>();
                for(int rowNum = 1; rowNum <= inputs.Count; rowNum++) {
                    RowData<string, object> rowData = inputs[rowNum - 1];

                    string p = "";
                    foreach(string columnName in columns) {
                        object data = rowData[columnName];

                        string encloser_open = "";
                        string encloser_close = "";
                        if(
                          data != null
                          && (
                            data.GetType() == typeof(System.String)
                            || data.GetType() == typeof(System.DateTime)
                          )
                        ) {
                            encloser_open = "'";
                            encloser_close = "'";
                        }
                        p += (p.Length > 0 ? "," : "") + encloser_open + (data == null ? "NULL" : data) + encloser_close;
                    }
                    p = "(" + p + ")";

                    sqlParams.Add(p);
                    if(rowNum % batchSize == 0 || (rowNum == inputs.Count && sqlParams.Count > 0)) {
                        command = new NpgsqlCommand(sql + String.Join(',', sqlParams), conn);
                        string loggingDetail = String.Join('\n', sqlParams).Replace("'", "");
                        try {
                            int affected = command.ExecuteNonQuery();
                        } catch(PostgresException e) {
                            if(e.Message.Contains("duplicate key value violates unique constraint")) {
                                Log.Logger.Warning("Duplicated value upon insert into " + tableName + ": " + e.Detail + "\nvalues: \n" + loggingDetail);
                            }
                        } catch(Exception e) {
                            Log.Logger.Error(e, "Error upon insert into " + tableName + " values: " + loggingDetail);
                            errors.Add(new DbInsertError() {
                                exception = e,
                                info = loggingDetail
                            });
                        }
                        sqlParams.Clear();
                    }
                }
            }

            return errors;
        }
    }
}
