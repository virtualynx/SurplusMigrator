using LinqKit;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using SurplusMigrator.Libraries;
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
        private ColumnType<ColumnName, string> columnTypes = null;
        public string[] ids;
        private int lastBatchSize = -1;
        private long dataCount = -1;
        private int fetchBatchMax = -1;
        private int fetchBatchCounter = 1;
        private bool isAlreadyTruncated = false;

        private const string OMIT_PATTERN_FOREIGN_KEY = "Key \\((.*)\\)=\\((.*)\\) is not present in table";
        private const string OMIT_PATTERN_NOT_NULL = "null value in column \"(.*)\" violates not-null constraint";

        public Table() { }

        public List<RowData<ColumnName, object>> getDatas(int batchSize) {
            List<RowData<ColumnName, object>> result = new List<RowData<ColumnName, object>> ();

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
                return new List<RowData<ColumnName, object>>();
            }

            MyConsole.Write("Batch-"+ fetchBatchCounter + "/"+ fetchBatchMax + "("+getProgressPercentage().ToString("0.0") + "%), fetch data from "+tableName+" ... ");
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
                    RowData<ColumnName, object> rowData = new RowData<ColumnName, object>();
                    for(int a = 0; a < columns.Length; a++) {
                        var value = dataReader.GetValue(dataReader.GetOrdinal(columns[a]));
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
            Console.WriteLine("Done (" + result.Count + " data fetched)");

            fetchBatchCounter++;

            return result;
        }

        public TaskInsertStatus insertData(List<RowData<ColumnName, object>> inputs, bool truncateBeforeInsert, bool autoGenerateId = false) {
            TaskInsertStatus result = new TaskInsertStatus();
            List<DbInsertFail> failures = new List<DbInsertFail>();
            result.errors = failures;

            if(truncateBeforeInsert && !isAlreadyTruncated && getDataCount() > 0) {
                truncate();
                isAlreadyTruncated = true;
            }

            //check & omit if attempted insert data is already in table
            if(!autoGenerateId) {
                omitDuplicatedData(failures, inputs);
                if(inputs.Count == 0) { //all data are duplicates
                    return result;
                }
            }

            //start inserting data
            string[] targetColumns;
            if(autoGenerateId) {
                targetColumns = columns.Where(a => ids.Any(b => b != a)).ToArray();
            } else {
                targetColumns = columns;
            }

            ColumnType<ColumnName, DataType> columnType = getColumnTypes();

            List<string> sqlParams = new List<string>();
            List<Dictionary<ParamNotation, TypedData>> sqlArguments = new List<Dictionary<ParamNotation, TypedData>>();
            long insertedCount = 0;
            int batchSize = 0;
            for(int rowNum = 1; rowNum <= inputs.Count; rowNum++) {
                RowData<ColumnName, object> rowData = inputs[rowNum - 1];

                string param = "";
                Dictionary<ParamNotation, TypedData> arg = new Dictionary<ParamNotation, TypedData>();
                foreach(string columnName in targetColumns) {
                    object data = rowData[columnName];
                    ParamNotation paramNotation = "@" + columnName + "_" + rowNum;
                    arg[paramNotation] = new TypedData() {
                        data = data == null ? DBNull.Value : data,
                        type = columnType[columnName]
                    };
                    param += (param.Length > 0 ? "," : "") + paramNotation;
                }
                param = "(" + param + ")";
                sqlParams.Add(param);
                sqlArguments.Add(arg);

                if(batchSize == 0) {
                    batchSize = 65535 / sqlArguments[0].Count;
                }

                //insert upon meeting the batch-quota
                if(rowNum % batchSize == 0 || (rowNum == inputs.Count && sqlParams.Count > 0)) {
                    bool retryInsert;
                    do {
                        if(sqlParams.Count == 0 && sqlArguments.Count == 0) { //occured when all data are omitted because of errors
                            break;
                        }
                        retryInsert = false;
                        long affectedRowCount = 0;

                        //List<string> loggingDetailArray = new List<string>();
                        if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                            throw new System.NotImplementedException();
                        } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                            NpgsqlConnection conn = (NpgsqlConnection)connection.GetDbConnection();

                            string sql = "INSERT INTO \"" + connection.GetDbLoginInfo().schema + "\".\"" + tableName + "\"(\"" + String.Join("\",\"", targetColumns) + "\") VALUES ";
                            NpgsqlCommand command = new NpgsqlCommand(sql + String.Join(',', sqlParams), conn);

                            foreach(Dictionary<ParamNotation, TypedData> argument in sqlArguments) {
                                //string q = "";
                                foreach(KeyValuePair<ParamNotation, TypedData> entry in argument) {
                                    TypedData typedData = entry.Value;
                                    postgreCommandAddParamWithValue(command, entry.Key, typedData.data);
                                    //q += (q.Length > 0 ? "," : "") + (typedData.data != null ? typedData.data.ToString().Replace("'", "\'") : "null");
                                }
                                //q = "(" + q + ")";
                                //loggingDetailArray.Add(q);
                            }
                            //string loggingDetail = String.Join('\n', loggingDetailArray);

                            try {
                                affectedRowCount = command.ExecuteNonQuery();
                                insertedCount += affectedRowCount;
                            } catch(PostgresException e) {
                                if(
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
                                } else if(e.Message.Contains("duplicate key value violates unique constraint")) {
                                    throw new Exception("Unique constraint violation upon insert into " + tableName + ": " + e.Detail);
                                } else {
                                    //Log.Logger.Error(e, "SQL error upon insert into " + tableName + ": " + e.Detail + "\nvalues: \n" + loggingDetail);
                                    MyConsole.Error(e, "SQL error upon insert into " + tableName + ": " + e.Detail);
                                    throw;
                                }
                            } catch(NpgsqlException e) {
                                if(e.Message == "A statement cannot have more than 65535 parameters") {
                                    int rowCount = sqlArguments.Count;
                                    int paramCount = sqlArguments[0].Count;
                                    MyConsole.Error("SQL error upon insert into " + tableName + ": " + e.Message + " (" + rowCount + " rows, " + paramCount + " params each row, " + (rowCount * paramCount) + " total params)");
                                }
                                throw;
                            } catch(Exception e) {
                                throw;
                            } finally {
                                command.Dispose();
                            }
                        }
                        result.successCount += affectedRowCount;
                        MyConsole.EraseLine();
                        MyConsole.Write(insertedCount + "/"+inputs.Count+" data inserted into " + tableName);
                    } while(retryInsert);
                    sqlParams.Clear();
                    sqlArguments.Clear();
                }
            }

            return result;
        }

        public double getProgressPercentage() {
            return (Convert.ToDouble(fetchBatchCounter) / Convert.ToDouble(fetchBatchMax)) * 100.00;
        }

        public long getDataCount() {
            if(dataCount == -1) {
                if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                    SqlConnection conn = (SqlConnection)connection.GetDbConnection();
                    SqlCommand command = new SqlCommand("SELECT COUNT(*) FROM " + connection.GetDbLoginInfo().schema + "." + tableName, conn);
                    SqlDataReader reader = command.ExecuteReader();
                    reader.Read();
                    dataCount = Convert.ToInt64(reader.GetValue(0));

                    reader.Close();
                    command.Dispose();
                } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                    NpgsqlConnection conn = (NpgsqlConnection)connection.GetDbConnection();
                    //NpgsqlCommand command = new NpgsqlCommand("SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = '" + connection.GetDbLoginInfo().schema + "." + tableName + "'::regclass", conn);
                    //NpgsqlDataReader reader = command.ExecuteReader();
                    //reader.Read();
                    //dataCount = Convert.ToInt64(reader.GetValue(0));

                    //NpgsqlCommand command = new NpgsqlCommand("VACUUM(ANALYZE, VERBOSE, FULL) \"" + connection.GetDbLoginInfo().schema + "\".\"" + tableName + "\"", conn);
                    //command.ExecuteNonQuery();
                    //command.Dispose();
                    //command = new NpgsqlCommand("SELECT COUNT(*) FROM " + connection.GetDbLoginInfo().schema + "." + tableName, conn);
                    
                    NpgsqlCommand command = new NpgsqlCommand("SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = '" + connection.GetDbLoginInfo().schema + "' AND relname = '" + tableName + "'", conn);
                    dataCount = (Int64)command.ExecuteScalar();

                    //reader.Close();
                    command.Dispose();
                }
            }

            return dataCount;
        }

        public ColumnType<ColumnName, DataType> getColumnTypes() {
            if(columnTypes == null) {
                columnTypes = new ColumnType<ColumnName, string>();

                if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                    SqlConnection conn = (SqlConnection)connection.GetDbConnection();
                    SqlCommand command = new SqlCommand("select [" + String.Join("],[", columns) + "] from [" + connection.GetDbLoginInfo().schema + "].[" + tableName + "]", conn);
                    SqlDataReader reader = command.ExecuteReader();

                    foreach(string columnName in columns) {
                        columnTypes.Add(columnName, reader.GetDataTypeName(reader.GetOrdinal(columnName)));
                    }

                    reader.Close();
                    command.Dispose();
                } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                    NpgsqlConnection conn = (NpgsqlConnection)connection.GetDbConnection();
                    NpgsqlCommand command = new NpgsqlCommand("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='" + connection.GetDbLoginInfo().schema + "' AND table_name = '" + tableName + "'", conn); ;
                    NpgsqlDataReader reader = command.ExecuteReader();

                    while(reader.Read()) {
                        string column_name = reader.GetValue(reader.GetOrdinal("column_name")).ToString();
                        string data_type = reader.GetValue(reader.GetOrdinal("data_type")).ToString();
                        columnTypes[column_name] = data_type;
                    }

                    reader.Close();
                    command.Dispose();
                }
            }

            return columnTypes;
        }

        private NpgsqlDbType getPostgreColumnType(string columnName) {
            ColumnType<ColumnName, DataType> columnTypes = getColumnTypes();
            NpgsqlDbType result = NpgsqlDbType.Unknown;

            if(columnTypes[columnName] == "integer") {
                result = NpgsqlDbType.Integer;
            } else if(columnTypes[columnName] == "numeric") {
                result = NpgsqlDbType.Numeric;
            } else if(columnTypes[columnName] == "character varying") {
                result = NpgsqlDbType.Varchar;
            } else if(columnTypes[columnName] == "text") {
                result = NpgsqlDbType.Text;
            } else if(columnTypes[columnName] == "timestamp without time zone") {
                result = NpgsqlDbType.Timestamp;
            } else if(columnTypes[columnName] == "jsonb") {
                result = NpgsqlDbType.Jsonb;
            } else if(columnTypes[columnName] == "boolean") {
                result = NpgsqlDbType.Boolean;
            }

            return result;
        }

        private void postgreCommandAddParamWithValue(NpgsqlCommand command, ParamNotation paramNotation, object data) {
            string columnName = getColumnNameFromParamNotation(paramNotation);
            NpgsqlDbType columnDbType = getPostgreColumnType(columnName);

            if(data.GetType() == typeof(decimal) && (columnDbType == NpgsqlDbType.Varchar || columnDbType == NpgsqlDbType.Text)) {
                data = data.ToString();
            }

            command.Parameters.AddWithValue(paramNotation, columnDbType, data);
        }

        private string getColumnNameFromParamNotation(ParamNotation paramNotation) {
            Match match = Regex.Match(paramNotation, "@(.*)_([0-9]+)");

            return match.Groups[1].Value;
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

        private void truncate(bool cascade = true) {
            string options = "";
            if(cascade) {
                options += " CASCADE";
            }
            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                throw new System.NotImplementedException();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                MyConsole.Information("Truncate \"" + connection.GetDbLoginInfo().schema + "\".\"" + tableName + "\"" + options);
                NpgsqlCommand command = new NpgsqlCommand("TRUNCATE TABLE \"" + connection.GetDbLoginInfo().schema + "\".\"" + tableName + "\"" + options, (NpgsqlConnection)connection.GetDbConnection());
                command.CommandTimeout = 300;
                command.ExecuteNonQuery();
                command.Dispose();
            }
        }

        private void omitDuplicatedData(List<DbInsertFail> failures, List<RowData<ColumnName, object>> inputs) {
            string sqlSelect = "select " + String.Join(",", ids) + " from \"" + connection.GetDbLoginInfo().schema + "\".\"" + tableName + "\" where (" + String.Join(",", ids) + ") in";
            List<string> sqlSelectParams = new List<string>();
            Dictionary<ParamNotation, object> sqlSelectArgs = new Dictionary<ParamNotation, object>();
            List<RowData<ColumnName, object>> selectResults = new List<RowData<ColumnName, object>>();
            for(int rowNum = 1; rowNum <= inputs.Count; rowNum++) {
                RowData<ColumnName, object> rowData = inputs[rowNum - 1];
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
                foreach(KeyValuePair<ParamNotation, object> entry in sqlSelectArgs) {
                    postgreCommandAddParamWithValue(command, entry.Key, entry.Value);
                }
                NpgsqlDataReader dataReader = command.ExecuteReader();
                while(dataReader.Read()) {
                    RowData<ColumnName, object> rowData = new RowData<ColumnName, object>();
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

            List<RowData<ColumnName, object>> duplicatedDatas = new List<RowData<ColumnName, object>>();
            foreach(RowData<ColumnName, object> rowSelect in selectResults) {
                var predicate = PredicateBuilder.New<RowData<ParamNotation, object>>();
                foreach(string id in ids) {
                    predicate = predicate.And(rowData => rowData[id].ToString().Trim() == rowSelect[id].ToString().Trim());
                }
                RowData<ColumnName, object> duplicate = inputs.Where(predicate).FirstOrDefault();
                if(duplicate != null) {
                    duplicatedDatas.Add(duplicate);
                    inputs.Remove(duplicate);
                    DbInsertFail insertfailInfo = new DbInsertFail() {
                        info = "Data already exists upon insert into " + tableName + ", value: " + JsonSerializer.Serialize(rowSelect),
                        severity = DbInsertFail.DB_FAIL_SEVERITY_WARNING
                    };
                    failures.Add(insertfailInfo);
                }
            }

            if(duplicatedDatas.Count > 0) {
                MyConsole.EraseLine();
                MyConsole.Warning("Skipping " + duplicatedDatas.Count + " duplicated data upon inserting into " + tableName);
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
                    exception = e,
                    info = "Foreignkey constraint violation upon insert into " + tableName + ", " + e.Detail + ", value: " + JsonSerializer.Serialize(arg),
                    severity = DbInsertFail.DB_FAIL_SEVERITY_ERROR
                };
                failures.Add(insertfailInfo);
            }

            if(filteredArguments.Count > 0) {
                MyConsole.EraseLine();
                MyConsole.Error("Error upon insert into " + tableName + ", " + e.Detail + "(" + filteredArguments.Count + " data)");
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
                    exception = e,
                    info = "Not-Null constraint violation upon insert into " + tableName + ", " + e.MessageText + ", value: " + JsonSerializer.Serialize(arg),
                    severity = DbInsertFail.DB_FAIL_SEVERITY_ERROR
                };
                failures.Add(insertfailInfo);
            }

            if(filteredArguments.Count > 0) {
                MyConsole.Error("Error upon insert into " + tableName + ", " + e.MessageText + "(" + filteredArguments.Count + " data)");
            }
        }
    }
}
