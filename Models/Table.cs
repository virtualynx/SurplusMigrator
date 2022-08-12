using LinqKit;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using Serilog;
using SurplusMigrator.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;

using ParamNotation = System.String;

namespace SurplusMigrator.Models
{
    class Table
    {
        public DbConnection_ connection;
        public string tableName;
        public string[] columns;
        public string[] ids;
        private int lastBatchSize = -1;
        private long dataCount = -1;
        private int fetchBatchMax = -1;
        private int fetchBatchCounter = 1;

        private const string OMIT_PATTERN_FOREIGN_KEY = "Key \\((.*)\\)=\\((.*)\\) is not present in table";
        private const string OMIT_PATTERN_NOT_NULL = "null value in column \"(.*)\" violates not-null constraint";

        public Table() { }

        public long getDataCount() {
            if(dataCount == -1) {
                SqlConnection conn = (SqlConnection)connection.GetDbConnection();
                SqlCommand command = new SqlCommand("SELECT COUNT(*) FROM " + connection.GetDbLoginInfo().schema + "." + tableName, conn);
                SqlDataReader dataReader = command.ExecuteReader();
                dataReader.Read();
                dataCount = Convert.ToInt64(dataReader.GetValue(0));

                dataReader.Close();
                command.Dispose();
            }

            return dataCount;
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

        public List<RowData<ColumnName, Data>> getDatas(int batchSize) {
            List<RowData<ColumnName, Data>> result = new List<RowData<ColumnName, Data>> ();

            //check for first time run/batchSize has changed
            if(lastBatchSize != batchSize) {
                lastBatchSize = batchSize;
                decimal d = Convert.ToDecimal(getDataCount()) / Convert.ToDecimal(batchSize);
                fetchBatchMax = (int)Math.Ceiling(d);
                fetchBatchCounter = 1;
            }

            //check if all batch already fetched
            if(fetchBatchMax != -1 && fetchBatchCounter > fetchBatchMax) {
                fetchBatchCounter = 1;
                return new List<RowData<ColumnName, Data>>();
            }

            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                SqlConnection conn = (SqlConnection)connection.GetDbConnection();

                SqlCommand command = null;
                SqlDataReader dataReader = null;

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

            fetchBatchCounter++;

            return result;
        }

        public List<DbInsertFail> insertData(List<RowData<ColumnName, Data>> inputs, int batchSize, bool autoGenerateId) {
            List<DbInsertFail> failures = new List<DbInsertFail>();

            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                throw new System.NotImplementedException();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                NpgsqlConnection conn = (NpgsqlConnection)connection.GetDbConnection();
                NpgsqlCommand command = null;

                //check if attempted insert data is already in table
                if(!autoGenerateId) {
                    omitDuplicatedData(failures, inputs);
                    if(inputs.Count == 0) { //all data are duplicates
                        return failures;
                    }
                }

                //start inserting data
                string[] targetColumns;
                if(autoGenerateId) {
                    targetColumns = columns.Where(a => ids.Any(b => b != a)).ToArray();
                } else {
                    targetColumns = columns;
                }
                string sql = "INSERT INTO \"" + connection.GetDbLoginInfo().schema + "\"." + tableName + "(\"" + String.Join("\",\"", targetColumns) + "\") VALUES ";
                ColumnType<ColumnName, DataType> columnType = this.getColumnTypes();
                List<string> sqlParams = new List<string>();
                List<Dictionary<ParamNotation, TypedData>> sqlArguments = new List<Dictionary<ParamNotation, TypedData>>();
                for(int rowNum = 1; rowNum <= inputs.Count; rowNum++) {
                    RowData<ColumnName, Data> rowData = inputs[rowNum - 1];

                    string p = "";
                    Dictionary<ParamNotation, TypedData> sqlArgument = new Dictionary<ParamNotation, TypedData>();
                    foreach(string columnName in targetColumns) {
                        object data = rowData[columnName];
                        ParamNotation paramNotation = "@" + columnName + "_" + rowNum;
                        sqlArgument.Add(
                            paramNotation, 
                            new TypedData() {
                                data = data == null ? DBNull.Value : data,
                                type = columnType[columnName]
                            }
                        );
                        p += (p.Length > 0 ? "," : "") + paramNotation;
                    }
                    p = "(" + p + ")";
                    sqlParams.Add(p);
                    sqlArguments.Add(sqlArgument);

                    //insert upon meeting the batch-quota
                    if(rowNum % batchSize == 0 || (rowNum == inputs.Count && sqlParams.Count > 0)) {
                        bool retryInsert;
                        do {
                            if(sqlParams.Count==0 && sqlArguments.Count == 0) { //occured when all data are omitted because of errors
                                break;
                            }
                            retryInsert = false;
                            command = new NpgsqlCommand(sql + String.Join(',', sqlParams), conn);
                            List<string> loggingDetailArray = new List<string>();
                            foreach(Dictionary<ParamNotation, TypedData> argument in sqlArguments) {
                                string q = "";
                                foreach(KeyValuePair<ParamNotation, TypedData> entry in argument) {
                                    TypedData typedData = entry.Value;
                                    if(typedData.type == "jsonb") {
                                        command.Parameters.AddWithValue(entry.Key, NpgsqlDbType.Json, typedData.data);
                                    } else if(typedData.type == "boolean") {
                                        command.Parameters.AddWithValue(entry.Key, NpgsqlDbType.Boolean, typedData.data);
                                    } else {
                                        command.Parameters.AddWithValue(entry.Key, typedData.data);
                                    }
                                    q += (q.Length > 0 ? "," : "") + (typedData.data != null ? typedData.data.ToString().Replace("'", "\'") : "null");
                                }
                                q = "(" + q + ")";
                                loggingDetailArray.Add(q);
                            }
                            string loggingDetail = String.Join('\n', loggingDetailArray);
                            try {
                                int affected = command.ExecuteNonQuery();
                                Log.Logger.Information(affected + " data inserted into " + tableName);
                            } catch(PostgresException e) {
                                if(e.Message.Contains("duplicate key value violates unique constraint")) {
                                    throw new System.NotImplementedException("This should not be happened in this point of code");
                                } else if(
                                    e.Message.Contains("insert or update on table")
                                    && e.Message.Contains("violates foreign key constraint")
                                    && e.Detail.Contains("Key")
                                    && e.Detail.Contains("is not present in table")
                                ) {
                                    omitForeignKeyViolationInsertData(e, failures, sqlParams, sqlArguments);
                                    retryInsert = true;
                                } else if(
                                    e.Message.Contains("null value in column")
                                    && e.Message.Contains("violates not-null constraint")
                                ) {
                                    omitNotNullViolationInsertData(e, failures, sqlParams, sqlArguments);
                                    retryInsert = true;
                                } else {
                                    Log.Logger.Error(e, "SQL error upon insert into " + tableName + ": " + e.Detail + "\nvalues: \n" + loggingDetail);
                                    failures.Add(new DbInsertFail() {
                                        exception = e,
                                        info = loggingDetail,
                                        severity = Misc.DB_FAIL_SEVERITY_ERROR,
                                        skipsNextInsertion = true
                                    });
                                }
                            } catch(Exception e) {
                                Log.Logger.Error(e, "Error upon insert into " + tableName + ", values: " + loggingDetail);
                                failures.Add(new DbInsertFail() {
                                    exception = e,
                                    info = loggingDetail,
                                    severity = Misc.DB_FAIL_SEVERITY_ERROR,
                                    skipsNextInsertion = true
                                });
                            } finally { 
                                command.Dispose();
                            }
                        } while(retryInsert);
                        sqlParams.Clear();
                        sqlArguments.Clear();
                    }
                }
            }

            return failures;
        }

        public bool setSequence(int num) {
            int affectedRow = 0;
            string sequenceName = tableName + "_" + ids[0] + "_seq";
            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                throw new System.NotImplementedException();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                NpgsqlCommand command = new NpgsqlCommand("ALTER SEQUENCE " + sequenceName + " RESTART WITH "+num, (NpgsqlConnection)connection.GetDbConnection());
                affectedRow = command.ExecuteNonQuery();
                command.Dispose();
            }

            return affectedRow > 0;
        }

        private void omitDuplicatedData(List<DbInsertFail> failures, List<RowData<ColumnName, Data>> inputs) {
            string sqlSelect = "select " + String.Join(",", ids) + " from " + connection.GetDbLoginInfo().schema + "." + tableName + " where (" + String.Join(",", ids) + ") in";
            List<string> sqlSelectParams = new List<string>();
            Dictionary<ParamNotation, Data> sqlSelectArgs = new Dictionary<ParamNotation, Data>();
            List<RowData<ColumnName, Data>> selectResults = new List<RowData<ColumnName, Data>>();
            for(int rowNum = 1; rowNum <= inputs.Count; rowNum++) {
                RowData<ColumnName, Data> rowData = inputs[rowNum - 1];
                List<string> paramTemp = new List<string>();
                foreach(string idColumn in ids) {
                    ParamNotation paramNotation = "@" + idColumn + "_" + rowNum;
                    paramTemp.Add(paramNotation);
                    sqlSelectArgs.Add(paramNotation, rowData[idColumn]);
                }
                sqlSelectParams.Add("(" + String.Join(",", paramTemp) + ")");
            }

            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                throw new System.NotImplementedException();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                NpgsqlCommand command = new NpgsqlCommand(sqlSelect + "(" + String.Join(',', sqlSelectParams) + ")", (NpgsqlConnection)connection.GetDbConnection());
                foreach(KeyValuePair<ParamNotation, Data> entry in sqlSelectArgs) {
                    command.Parameters.AddWithValue(entry.Key, entry.Value);
                }
                NpgsqlDataReader dataReader = command.ExecuteReader();
                while(dataReader.Read()) {
                    RowData<ColumnName, Data> rowData = new RowData<ColumnName, Data>();
                    foreach(string id in ids) {
                        var value = dataReader.GetValue(dataReader.GetOrdinal(id));
                        if(value.GetType() == typeof(System.DBNull)) {
                            value = null;
                        } else if(value.GetType() == typeof(string)) {
                            value = value.ToString().Trim();
                        }
                        rowData.Add(id, value);
                    }
                    selectResults.Add(rowData);
                }
                dataReader.Close();
                command.Dispose();
            }

            List<RowData<ColumnName, Data>> duplicatedDatas = new List<RowData<ColumnName, Data>>();
            foreach(RowData<ColumnName, Data> rowSelect in selectResults) {
                var predicate = PredicateBuilder.New<RowData<ParamNotation, Data>>();
                foreach(string id in ids) {
                    predicate = predicate.And(rowData => rowData[id].ToString().Trim() == rowSelect[id].ToString().Trim());
                }
                RowData<ColumnName, Data> duplicate = inputs.Where(predicate).FirstOrDefault();
                if(duplicate != null) {
                    duplicatedDatas.Add(duplicate);
                    inputs.Remove(duplicate);
                    DbInsertFail insertfailInfo = new DbInsertFail() {
                        info = "Data already exists upon insert into " + tableName + ", value: " + JsonSerializer.Serialize(rowSelect),
                        severity = Misc.DB_FAIL_SEVERITY_WARNING
                    };
                    failures.Add(insertfailInfo);
                }
            }

            int duplicateCount = failures.Where(a => a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
            if(duplicateCount > 0) {
                Log.Logger.Warning("Skipping " + duplicateCount + " duplicated data upon inserting into " + tableName);
            }
        }

        private void omitForeignKeyViolationInsertData(
            PostgresException e,
            List<DbInsertFail> failures,
            List<string> sqlParams,
            List<Dictionary<ParamNotation, TypedData>> sqlArguments
        ) {
            Match match = Regex.Match(e.Detail, OMIT_PATTERN_FOREIGN_KEY);
            string column = match.Groups[1].Value;
            string id = match.Groups[2].Value;

            List<Dictionary<ParamNotation, TypedData>> filteredArguments = sqlArguments.Where(
                arg => arg.Any(x => x.Key.StartsWith("@" + column + "_") && x.Value.data.ToString() == id)
            ).ToList();

            foreach(Dictionary<ParamNotation, TypedData> arg in filteredArguments) {
                string sqlParamKey = null;
                foreach(string k in arg.Keys) {
                    if(k.StartsWith("@" + column + "_")) {
                        sqlParamKey = k;
                        break;
                    }
                }
                if(sqlParams.Any()) { //prevent IndexOutOfRangeException for empty list
                    sqlParams.RemoveAll(a => a.Contains(sqlParamKey + ","));
                }
                if(sqlArguments.Any()) {
                    sqlArguments.RemoveAll(arg => arg.Any(x => x.Key == sqlParamKey));
                }
                DbInsertFail insertfailInfo = new DbInsertFail() {
                    info = "Foreignkey constraint violation upon insert into " + tableName + ", " + e.Detail + ", value: " + JsonSerializer.Serialize(arg),
                    severity = Misc.DB_FAIL_SEVERITY_ERROR
                };
                failures.Add(insertfailInfo);
            }

            int failedCount = failures.Where(a => a.info.StartsWith("Foreignkey constraint violation upon insert into ")).ToList().Count;
            if(failedCount > 0) {
                Log.Logger.Error("Error upon insert into " + tableName + ", " + e.Detail + "(" + failedCount + " data)");
            }
        }

        private void omitNotNullViolationInsertData(
            PostgresException e,
            List<DbInsertFail> failures,
            List<string> sqlParams,
            List<Dictionary<ParamNotation, TypedData>> sqlArguments
        ) {
            Match match = Regex.Match(e.MessageText, OMIT_PATTERN_NOT_NULL);
            string column = match.Groups[1].Value;

            List<Dictionary<ParamNotation, TypedData>> filteredArguments = sqlArguments.Where(
                arg => arg.Any(x => x.Key.StartsWith("@" + column + "_") && (x.Value.data == DBNull.Value))
            ).ToList();

            foreach(Dictionary<ParamNotation, TypedData> arg in filteredArguments) {
                string sqlParamKey = null;
                foreach(string k in arg.Keys) {
                    if(k.StartsWith("@" + column + "_")) {
                        sqlParamKey = k;
                        break;
                    }
                }
                if(sqlParams.Any()) { //prevent IndexOutOfRangeException for empty list
                    sqlParams.RemoveAll(a => a.Contains(sqlParamKey + ","));
                }
                if(sqlArguments.Any()) {
                    sqlArguments.RemoveAll(arg => arg.Any(x => x.Key == sqlParamKey));
                }
                DbInsertFail insertfailInfo = new DbInsertFail() {
                    info = "Not-Null constraint violation upon insert into " + tableName + ", " + e.MessageText + ", value: " + JsonSerializer.Serialize(arg),
                    severity = Misc.DB_FAIL_SEVERITY_ERROR
                };
                failures.Add(insertfailInfo);
            }

            int failedCount = failures.Where(a => a.info.StartsWith("Not-Null constraint violation upon insert into ")).ToList().Count;
            if(failedCount > 0) {
                Log.Logger.Error("Error upon insert into " + tableName + ", " + e.MessageText + "(" + failedCount + " data)");
            }
        }
    }
}
