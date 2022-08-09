using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using Serilog;
using SurplusMigrator.Interfaces;
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
        private long dataCount = -1;
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

        private ColumnType<ColumnName, DataType> getColumnTypes() {
            ColumnType<ColumnName, string> result = new ColumnType<ColumnName, string>();

            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                throw new System.NotImplementedException();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                NpgsqlConnection conn = (NpgsqlConnection)connection.GetDbConnection();

                NpgsqlCommand command = new NpgsqlCommand("select " + String.Join(',', columns) + " from " + connection.GetDbLoginInfo().schema + "." + tableName, conn); ;
                NpgsqlDataReader reader = command.ExecuteReader();

                foreach(string columnName in columns) {
                    result.Add(columnName, reader.GetDataTypeName(reader.GetOrdinal(columnName)));
                }

                reader.Close();
                command.Dispose();
            }

            return result;
        }

        public List<RowData<ColumnName, Data>> getDatas() {
            List<RowData<ColumnName, Data>> result = new List<RowData<ColumnName, Data>> ();

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
                if(dataCount == -1) {
                    command = new SqlCommand(
                        "SELECT COUNT(*) FROM " + connection.GetDbLoginInfo().schema + "." + tableName
                        , conn
                    );
                    dataReader = command.ExecuteReader();
                    dataReader.Read();
                    dataCount = Convert.ToInt64(dataReader.GetValue(0));

                    dataReader.Close();
                    command.Dispose();

                    decimal d = Convert.ToDecimal(dataCount) / Convert.ToDecimal(batchSize);
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
                    RowData<ColumnName, Data> rowData = new RowData<ColumnName, Data>();
                    for(int a = 0; a < columns.Length; a++) {
                        var value = dataReader.GetValue(a);
                        if(value.GetType() == typeof(System.DBNull)) {
                            value = null;
                        } else if(value.GetType() == typeof(string)) {
                            value = value.ToString().Trim();
                        }
                        rowData.Add(columns[a], value);
                    }
                    result.Add(rowData);
                }

                dataReader.Close();
                command.Dispose();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                throw new System.NotImplementedException();
            }

            if(result.Count > 0) {
                fetchBatchCounter++;
            }

            return result;
        }

        //public List<DbInsertError> insertData(List<RowData<ColumnName, Data>> inputs) {
        //    List<DbInsertError> errors = new List<DbInsertError>();

        //    if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
        //        throw new System.NotImplementedException();
        //    } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
        //        NpgsqlConnection conn = (NpgsqlConnection)connection.GetDbConnection();

        //        NpgsqlCommand command = null;

        //        string sql = "INSERT INTO \"" + connection.GetDbLoginInfo().schema + "\"." + tableName + "(\"" + String.Join("\",\"", columns) + "\") VALUES ";
        //        List<string> sqlParams = new List<string>();
        //        for(int rowNum = 1; rowNum <= inputs.Count; rowNum++) {
        //            RowData<ColumnName, Data> rowData = inputs[rowNum - 1];

        //            string p = "";
        //            foreach(string columnName in columns) {
        //                object data = rowData[columnName];

        //                string encloser_open = "";
        //                string encloser_close = "";
        //                if(
        //                  data != null
        //                  && (
        //                    data.GetType() == typeof(System.String)
        //                    || data.GetType() == typeof(System.DateTime)
        //                  )
        //                ) {
        //                    encloser_open = "'";
        //                    encloser_close = "'";
        //                }
        //                p += (p.Length > 0 ? "," : "") + encloser_open + (data == null ? "NULL" : data) + encloser_close;
        //            }
        //            p = "(" + p + ")";

        //            sqlParams.Add(p);
        //            if(rowNum % batchSize == 0 || (rowNum == inputs.Count && sqlParams.Count > 0)) {
        //                command = new NpgsqlCommand(sql + String.Join(',', sqlParams), conn);
        //                string loggingDetail = String.Join('\n', sqlParams).Replace("'", "");
        //                try {
        //                    int affected = command.ExecuteNonQuery();
        //                } catch(PostgresException e) {
        //                    if(e.Message.Contains("duplicate key value violates unique constraint")) {
        //                        Log.Logger.Warning("Duplicated value upon insert into " + tableName + ": " + e.Detail + "\nvalues: \n" + loggingDetail);
        //                    } else {
        //                        Log.Logger.Warning("SQL error upon insert into " + tableName + ": " + e.Detail + "\nvalues: \n" + loggingDetail);
        //                        errors.Add(new DbInsertError() {
        //                            exception = e,
        //                            info = loggingDetail
        //                        });
        //                    }
        //                } catch(Exception e) {
        //                    Log.Logger.Error(e, "Error upon insert into " + tableName + " values: " + loggingDetail);
        //                    errors.Add(new DbInsertError() {
        //                        exception = e,
        //                        info = loggingDetail
        //                    });
        //                }
        //                sqlParams.Clear();
        //            }
        //        }
        //    }

        //    return errors;
        //}

        public List<DbInsertError> insertData(List<RowData<ColumnName, Data>> inputs) {
            List<DbInsertError> errors = new List<DbInsertError>();

            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                throw new System.NotImplementedException();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                NpgsqlConnection conn = (NpgsqlConnection)connection.GetDbConnection();

                NpgsqlCommand command = null;

                string sql = "INSERT INTO \"" + connection.GetDbLoginInfo().schema + "\"." + tableName + "(\"" + String.Join("\",\"", columns) + "\") VALUES ";
                ColumnType<ColumnName, DataType> columnType = this.getColumnTypes();
                List<string> sqlParams = new List<string>();
                List<Dictionary<string, object>> sqlArguments = new List<Dictionary<string, object>>();
                for(int rowNum = 1; rowNum <= inputs.Count; rowNum++) {
                    RowData<ColumnName, Data> rowData = inputs[rowNum - 1];

                    string p = "";
                    Dictionary<string, object> sqlArgument = new Dictionary<string, object>();
                    foreach(string columnName in columns) {
                        object data = rowData[columnName];
                        string paramNotation = "@" + columnName + "_" + rowNum;
                        sqlArgument.Add(columnName + "_" + rowNum, data == null ? DBNull.Value : data);
                        p += (p.Length > 0 ? "," : "") + paramNotation;
                    }
                    p = "(" + p + ")";
                    sqlParams.Add(p);
                    sqlArguments.Add(sqlArgument);

                    if(rowNum % batchSize == 0 || (rowNum == inputs.Count && sqlParams.Count > 0)) {
                        command = new NpgsqlCommand(sql + String.Join(',', sqlParams), conn);
                        List<string> loggingDetailArray = new List<string>();
                        foreach(Dictionary<string, object> argument in sqlArguments) {
                            string q = "";
                            foreach(KeyValuePair<string, object> entry in argument) {
                                if(columnType[entry.Key] == "jsonb") {
                                    command.Parameters.AddWithValue("@" + entry.Key, NpgsqlDbType.Json, entry.Value);
                                } else {
                                    command.Parameters.AddWithValue("@" + entry.Key, entry.Value);
                                }
                                q += (q.Length > 0 ? "," : "") + ( entry.Value!=null? entry.Value.ToString().Replace("'", "\'"): "null" );
                            }
                            q = "(" + q + ")";
                            loggingDetailArray.Add(q);
                        }
                        string loggingDetail = String.Join('\n', loggingDetailArray);
                        try {
                            int affected = command.ExecuteNonQuery();
                        } catch(PostgresException e) {
                            if(e.Message.Contains("duplicate key value violates unique constraint")) {
                                Log.Logger.Warning("Duplicated value upon insert into " + tableName + ": " + e.Detail + "\nvalues: \n" + loggingDetail);
                            } else {
                                Log.Logger.Error(e, "SQL error upon insert into " + tableName + ": " + e.Detail + "\nvalues: \n" + loggingDetail);
                                errors.Add(new DbInsertError() {
                                    exception = e,
                                    info = loggingDetail
                                });
                            }
                        } catch(Exception e) {
                            Log.Logger.Error(e, "Error upon insert into " + tableName + " values: " + loggingDetail);
                            errors.Add(new DbInsertError() {
                                exception = e,
                                info = loggingDetail
                            });
                        }
                        sqlParams.Clear();
                        sqlArguments.Clear();
                    }
                }
            }

            return errors;
        }
    }
}
