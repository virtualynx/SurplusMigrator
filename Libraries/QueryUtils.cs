using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using Npgsql;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Data.Common;

namespace SurplusMigrator.Libraries {
    class QueryUtils {
        private class BatchInfo {
            public DbLoginInfo dbLoginInfo;
            public string tablename;
            public int dataCount = -1;
            public int dataRead = 0;
        }

        private static List<BatchInfo> batchInfos = new List<BatchInfo>();

        public static string[] getPrimaryKeys(DbConnection_ connection, string tablename) {
            List<string> result = new List<string>();

            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                throw new NotImplementedException();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                string query = @"
                    SELECT               
                        pg_attribute.attname, 
                        format_type(pg_attribute.atttypid, pg_attribute.atttypmod) 
                    FROM 
                        pg_index, 
                        pg_class, 
                        pg_attribute, 
                        pg_namespace 
                    WHERE 
                      pg_class.oid = '""[tablename]""'::regclass AND 
                      indrelid = pg_class.oid AND 
                      nspname = '[schema]' AND 
                      pg_class.relnamespace = pg_namespace.oid AND 
                      pg_attribute.attrelid = pg_class.oid AND 
                      pg_attribute.attnum = any(pg_index.indkey)
                     AND indisprimary
                    ;
                ";
                query = query.Replace("[schema]", connection.GetDbLoginInfo().schema);
                query = query.Replace("[tablename]", tablename);

                NpgsqlCommand command = new NpgsqlCommand(query, (NpgsqlConnection)connection.GetDbConnection());
                NpgsqlDataReader reader = command.ExecuteReader();
                while(reader.Read()) {
                    dynamic data = reader.GetValue(reader.GetOrdinal("attname"));
                    if(data.GetType() == typeof(System.DBNull)) {
                        data = null;
                    } else if(data.GetType() == typeof(string)) {
                        data = data.ToString().Trim();
                    }
                    result.Add(data);
                }
                reader.Close();
                command.Dispose();

                return result.ToArray();
            }

            throw new NotImplementedException("Only SQL-Server and PostgreSql database is supported");
        }

        public static string[] getColumnNames(DbConnection_ connection, string tablename) {
            List<string> result = new List<string>();

            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                string query = @"
                    SELECT 
	                    COLUMN_NAME
                    FROM 
	                    INFORMATION_SCHEMA.COLUMNS
                    WHERE 
	                    TABLE_SCHEMA = '<schema>'
	                    and TABLE_NAME = '<tablename>'
                    ;
                ";
                query = query.Replace("<schema>", connection.GetDbLoginInfo().schema);
                query = query.Replace("<tablename>", tablename);

                SqlCommand command = new SqlCommand(query, (SqlConnection)connection.GetDbConnection());
                SqlDataReader reader = command.ExecuteReader();

                while(reader.Read()) {
                    dynamic data = reader.GetValue(reader.GetOrdinal("COLUMN_NAME"));
                    if(data.GetType() == typeof(System.DBNull)) {
                        data = null;
                    } else if(data.GetType() == typeof(string)) {
                        data = data.ToString().Trim();
                    }
                    result.Add(data);
                }
                reader.Close();
                command.Dispose();

                return result.ToArray();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                string query = @"
                    select 
                        attname
                    from 
                        pg_attribute
                    where 
                        attnum > 0
                        AND NOT attisdropped
                        and attrelid = (
    	                    select 
			                    s.oid 
		                    from 
			                    pg_class s
			                    join pg_namespace sn on sn.oid = s.relnamespace
		                    where 
			                    sn.nspname = '[schema]'
			                    and relname = '[tablename]'
                        )
                    ;
                ";
                query = query.Replace("[schema]", connection.GetDbLoginInfo().schema);
                query = query.Replace("[tablename]", tablename);

                NpgsqlCommand command = new NpgsqlCommand(query, (NpgsqlConnection)connection.GetDbConnection());
                NpgsqlDataReader reader = command.ExecuteReader();
                while(reader.Read()) {
                    dynamic data = reader.GetValue(reader.GetOrdinal("attname"));
                    if(data.GetType() == typeof(System.DBNull)) {
                        data = null;
                    } else if(data.GetType() == typeof(string)) {
                        data = data.ToString().Trim();
                    }
                    result.Add(data);
                }
                reader.Close();
                command.Dispose();

                return result.ToArray();
            }

            throw new NotImplementedException("Only SQL-Server and PostgreSql database is supported");
        }

        public static RowData<ColumnName, object>[] executeQuery(DbConnection_ connection, string sql, Dictionary<string, object> parameters = null, DbTransaction transaction = null, int timeoutSeconds = 30) {
            List<RowData<ColumnName, object>> result = new List<RowData<string, dynamic>>();

            if(parameters != null) {
                foreach(var map in parameters) {
                    if(map.Value != null && (map.Value.GetType().IsArray || Utils.isList(map.Value))) {
                        object[] valueArr;
                        if(map.Value.GetType().IsArray) {
                            valueArr = (object[])map.Value;
                        } else {
                            throw new NotImplementedException();
                        }
                        var valueList = (from v in valueArr select getInsertArg(v)).ToArray();
                        sql = sql.Replace(map.Key, "(" + String.Join(",", valueList) + ")");
                    } else {
                        sql = sql.Replace(map.Key, getInsertArg(map.Value));
                    }
                }
            }

            bool retry;
            do {
                retry = false;
                if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                    SqlCommand command = new SqlCommand(sql, (SqlConnection)connection.GetDbConnection());
                    if(transaction != null) {
                        command.Transaction = (SqlTransaction)transaction;
                    }
                    if(timeoutSeconds != -1) {
                        command.CommandTimeout = timeoutSeconds;
                    }
                    try {
                        SqlDataReader reader = command.ExecuteReader();
                        while(reader.Read()) {
                            RowData<ColumnName, dynamic> rowData = new RowData<ColumnName, dynamic>();
                            for(int a = 0; a < reader.FieldCount; a++) {
                                string columnName = reader.GetName(a);
                                dynamic data = reader.GetValue(a);
                                if(data.GetType() == typeof(System.DBNull)) {
                                    data = null;
                                } else if(data.GetType() == typeof(string)) {
                                    data = data.ToString().Trim();
                                }
                                rowData.Add(columnName, data);
                            }
                            result.Add(rowData);
                        }
                        reader.Close();
                    } catch(Exception e) {
                        if(isConnectionProblem(e)) {
                            retry = true;
                        } else {
                            throw;
                        }
                    } finally {
                        command.Dispose();
                    }
                } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                    NpgsqlCommand command = new NpgsqlCommand(sql, (NpgsqlConnection)connection.GetDbConnection());
                    if(transaction != null) {
                        command.Transaction = (NpgsqlTransaction)transaction;
                    }
                    if(timeoutSeconds != -1) {
                        command.CommandTimeout = timeoutSeconds;
                    }
                    try {
                        NpgsqlDataReader reader = command.ExecuteReader();
                        while(reader.Read()) {
                            RowData<ColumnName, dynamic> rowData = new RowData<ColumnName, dynamic>();
                            for(int a = 0; a < reader.FieldCount; a++) {
                                string columnName = reader.GetName(a);
                                dynamic data = reader.GetValue(a);
                                if(data.GetType() == typeof(System.DBNull)) {
                                    data = null;
                                } else if(data.GetType() == typeof(string)) {
                                    data = data.ToString().Trim();
                                }
                                rowData.Add(columnName, data);
                            }
                            result.Add(rowData);
                        }
                        reader.Close();
                    } catch(Exception e) {
                        if(isConnectionProblem(e)) {
                            retry = true;
                        } else {
                            throw;
                        }
                    } finally {
                        command.Dispose();
                    }
                }
            } while(retry);

            return result.ToArray();
        }

        public static int getDataCount(DbConnection_ connection, string tablename, string whereClauses = null) {
            char[] dbObjectEnclosers = new char[] { };
            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                dbObjectEnclosers = new char[] { '[', ']' };
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                dbObjectEnclosers = new char[] { '"', '"' };
            }

            string queryCount = "select count(1) as datacount from " + dbObjectEnclosers[0] + connection.GetDbLoginInfo().schema + dbObjectEnclosers[1] + "." + dbObjectEnclosers[0] + tablename + dbObjectEnclosers[1];
            if(whereClauses != null) {
                queryCount = queryCount + " WHERE " +whereClauses;
            }
            var count = executeQuery(connection, queryCount);

            return Utils.obj2int(count[0]["datacount"]);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tablename"></param>
        /// <param name="onlyApplicationGeneratedData"></param>
        /// <param name="batchSize"></param>
        /// <param name="ids"></param>
        /// <param name="whereClauses">The optional where clause(s)</param>
        /// <param name="verbose"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public static RowData<ColumnName, object>[] getDataBatch(
            DbConnection_ connection,
            string tablename,
            string whereClauses = null,
            int batchSize = 10000,
            string[] ids = null,
            bool verbose = false
        ) {
            var batchInfo = batchInfos.Where(a => 
                a.dbLoginInfo == connection.GetDbLoginInfo() 
                && a.tablename == tablename
            ).FirstOrDefault();

            if(batchInfo == null) {
                batchInfo = new BatchInfo() {
                    dbLoginInfo = connection.GetDbLoginInfo(),
                    tablename = tablename,
                    dataCount = getDataCount(connection, tablename, whereClauses),
                    dataRead = 0
                };
                batchInfos.Add(batchInfo);
            } else if(batchInfo.dataRead >= batchInfo.dataCount) {
                batchInfo.dataCount = getDataCount(connection, tablename, whereClauses);
                batchInfo.dataRead = 0;

                return new RowData<ColumnName, object>[] { };
            }

            var columns = getColumnNames(connection, tablename);
            if(ids == null) {
                ids = getPrimaryKeys(connection, tablename);
            }

            string query;
            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                query = @"
                    SELECT 
                        <selected_columns> 
                    FROM    ( 
                                SELECT    
                                    ROW_NUMBER() OVER ( ORDER BY <over_orderby> ) AS RowNum
                                    , *
                                FROM      
                                    [<schema>].[<tablename>]
                                <where_clauses>
                            ) AS RowConstrainedResult
                    WHERE   
                        RowNum >= <offset_start>
                        AND RowNum <= <offset_end>
                    ORDER BY RowNum";

                query = query.Replace("<selected_columns>", "[" + String.Join("],[", columns) + "]");

                string over_orderby = "(select null)";
                if(ids != null && ids.Length > 0) {
                    over_orderby = String.Join(',', ids);
                }
                query = query.Replace("<over_orderby>", over_orderby);

                query = query.Replace("<schema>", connection.GetDbLoginInfo().schema);
                query = query.Replace("<tablename>", tablename);

                query = query.Replace("<offset_start>", (batchInfo.dataRead + 1).ToString());
                query = query.Replace("<offset_end>", (batchInfo.dataRead + batchSize).ToString());
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                query = @"
                    select 
                        <columns> 
                    from 
                        ""<schema_name>"".""<tablename>""
                    <where_clauses>
                    <order_by>
                    <offset_limit>
                ";

                query = query.Replace("<columns>", "\"" + String.Join("\",\"", columns) + "\"");
                query = query.Replace("<schema_name>", connection.GetDbLoginInfo().schema);
                query = query.Replace("<tablename>", tablename);

                string orderBy = " order by ";
                if(ids.Length > 0) {
                    orderBy += "\"" + String.Join("\",\"", ids) + "\"";
                } else {
                    orderBy += "\"" + String.Join("\",\"", columns) + "\"";
                }
                query = query.Replace("<order_by>", orderBy);

                query = query.Replace("<offset_limit>", " offset " + batchInfo.dataRead + " limit " + batchSize);
            } else {
                throw new NotImplementedException("Unknown database implementation");
            }

            query = query.Replace("<where_clauses>", whereClauses!=null? "WHERE "+ whereClauses: "");

            var rs = executeQuery(connection, query);

            int readUntil = batchInfo.dataRead + batchSize;
            readUntil = readUntil > batchInfo.dataCount ? batchInfo.dataCount : readUntil;
            if(verbose) {
                //MyConsole.WriteLine("Read batch data " + tablename + " " + batchInfo.dataRead + " - " + readUntil + " / " + batchInfo.dataCount);
                MyConsole.WriteLine("Read batch data " + tablename + " " + readUntil + " / " + batchInfo.dataCount);
            }

            batchInfo.dataRead = readUntil;

            return rs.ToArray();
        }

        public static void toggleTrigger(DbConnection_ connection, string tablename, bool enable) {
            string enableStr = enable ? "ENABLE" : "DISABLE";
            executeQuery(connection, "ALTER TABLE \""+ connection .GetDbLoginInfo().schema+ "\".\"" + tablename + "\" "+ enableStr + " TRIGGER ALL;");
        }

        /// <summary>
        /// Only works for postgresql server
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="table"></param>
        /// <param name="columns"></param>
        /// <param name="filters"></param>
        /// <param name="exact"></param>
        /// <returns></returns>
        public static RowData<ColumnName, object>[] searchSimilarity(DbConnection_ connection, string table, string[] columns, Dictionary<string, dynamic> filters, double index) {
            string sql = @"
                select
                    [column]
                from
                    [table]
            ";

            bool exact = index >= 1.0;

            List<string> columnsTemp = new List<string>(columns);
            List<string> filtersForSql = new List<string>();
            List<string> orderForSql = new List<string>();
            foreach(KeyValuePair<string, dynamic> entry in filters) {
                if(exact) {
                    filtersForSql.Add(entry.Key + " = @" + entry.Key);
                } else {
                    filtersForSql.Add(entry.Key + " % @" + entry.Key);
                    filtersForSql.Add(entry.Key + " <-> @" + entry.Key + " >= "+ index);
                    columnsTemp.Add(entry.Key + " <-> @" + entry.Key + " as " + entry.Key + "_similarity");
                    orderForSql.Add(entry.Key + "_similarity desc");
                }
            }
            columns = columnsTemp.ToArray();

            if(filtersForSql.Count > 0) {
                sql += " where " + String.Join(" and ", filtersForSql);
                if(!exact && orderForSql.Count > 0) {
                    sql += " order by " + String.Join(",", orderForSql);
                }
            }

            sql = sql.Replace("[column]", String.Join(",", columns));
            sql = sql.Replace("[table]", table);

            NpgsqlConnection conn = (NpgsqlConnection)connection.GetDbConnection();
            NpgsqlCommand command = new NpgsqlCommand(sql, conn);

            foreach(KeyValuePair<string, dynamic> entry in filters) {
                command.Parameters.AddWithValue("@" + entry.Key, entry.Value);
            }

            List<RowData<ColumnName, object>> results = new List<RowData<ColumnName, object>>();
            NpgsqlDataReader reader = command.ExecuteReader();
            while(reader.Read()) {
                RowData<ColumnName, dynamic> rowData = new RowData<ColumnName, dynamic>();
                for(int a = 0; a < reader.FieldCount; a++) {
                    string columnName = reader.GetName(a);
                    dynamic data = reader.GetValue(a);
                    if(data.GetType() == typeof(System.DBNull)) {
                        data = null;
                    } else if(data.GetType() == typeof(string)) {
                        data = data.ToString().Trim();
                    }
                    rowData.Add(columnName, data);
                }
                results.Add(rowData);
            }
            reader.Close();
            command.Dispose();

            return results.ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="targetTable"></param>
        /// <param name="selectColumns"></param>
        /// <param name="targetColumn"></param>
        /// <param name="word"></param>
        /// <param name="maxResult"></param>
        /// <returns></returns>
        public static RowData<ColumnName, object>[] searchSimilar(
            DbConnection_ connection,
            string targetTable,
            string[] selectColumns,
            string targetColumn,
            string word,
            int maxResult = 1
        ) {
            RowData<ColumnName, object>[] queryResult;

            string[] wordSplit = word.Split(" ");
            int wordCounter = 1;
            bool targetColumnSelected = selectColumns.Any(a => a == targetColumn);
            do {
                string sql = @"
                    select
                        <columns>
                    from
                        ""<schema>"".""<tables>""
                    where
                        <filters>
                ";
                List<string> filters = new List<string>();
                for(int a = 0; a < wordCounter; a++) {
                    string splittedWord = wordSplit[a].ToLower();
                    splittedWord = splittedWord.Replace("'", "''");
                    splittedWord = splittedWord.Replace("\\", "\\\\");
                    filters.Add("lower(\"" + targetColumn + "\") like '%" + splittedWord + "%'");
                }

                if(!targetColumnSelected && !selectColumns.Contains(targetColumn)) { //target column is needed for indexing later
                    selectColumns = selectColumns.Append(targetColumn).ToArray();
                }
                sql = sql.Replace("<columns>", "\"" + String.Join("\",\"", selectColumns) + "\"");
                sql = sql.Replace("<schema>", connection.GetDbLoginInfo().schema);
                sql = sql.Replace("<tables>", targetTable);
                sql = sql.Replace("<filters>", String.Join(" and ", filters));
                queryResult = QueryUtils.executeQuery(connection, sql);
                wordCounter++;
            } while(queryResult.Length > maxResult && wordCounter <= wordSplit.Length);

            foreach(var row in queryResult) {
                int containsCount = 0;
                string targetData = Utils.obj2str(row[targetColumn]);
                foreach(var w in wordSplit) {
                    if(targetData.ToLower().Contains(w.ToLower())) {
                        containsCount++;
                    }
                }
                double index = containsCount / wordSplit.Length;
                row["similarity_index"] = index;
            }

            //sort by similarity_index desc
            Array.Sort(queryResult, (x, y) => {
                double y_index = Double.Parse(y["similarity_index"].ToString());
                double x_index = Double.Parse(x["similarity_index"].ToString());

                if(y_index > x_index) return -1;
                if(y_index < x_index) return 1;
                return 0;
            });

            foreach(var row in queryResult) {
                //row.Remove("similarity_index");
                if(!targetColumnSelected && selectColumns.Contains(targetColumn)) {
                    row.Remove(targetColumn);
                }
            }

            return queryResult;
        }

        public static string getInsertArg(object data, Type targetType = null) {
            string convertedData = null;

            Type type = data?.GetType();
            if(data == null || type == typeof(DBNull)) {
                convertedData = "NULL";
            } else if(type == typeof(string)) {
                string dataStr = data.ToString();
                dataStr = dataStr.Replace("'", "''");
                dataStr = dataStr.Replace("\\", "\\\\");
                convertedData = "'" + dataStr + "'";
            } else if(type == typeof(bool)) {
                convertedData = data.ToString().ToLower();
            } else if(type == typeof(DateTime)) {
                convertedData = "'" + ((DateTime)data).ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            } else if(type == typeof(TimeSpan)) {
                convertedData = "'" + data.ToString() + "'";
            } else if(
                type == typeof(short)
                || type == typeof(int) 
                || type == typeof(long)
                || type == typeof(double)
                || type == typeof(decimal)
            ) {
                convertedData = data.ToString();
            } else {
                throw new Exception("Unknown data type: "+type?.ToString()+", value: "+data?.ToString());
            }

            return convertedData;
        }

        public static bool isConnectionProblem(Exception e) {
            bool result = false;

            string detailedInfo = null;
            if(
                e.Message.Contains("The timeout period elapsed prior to completion of the operation or the server is not responding")
                && e.InnerException.Message == "The wait operation timed out."
            ) {
                result = true;
                detailedInfo = e.InnerException.Message;
            }
            if(
                e.Message.Contains("A network-related or instance-specific error occurred while establishing a connection to SQL Server")
                && e.Message.Contains("established connection failed because connected host has failed to respond")
            ) {
                result = true;
                detailedInfo = "Connected host has failed to respond";
            }
            if(
                e.Message.Contains("A transport-level error has occurred when sending the request to the server")
                && e.Message.Contains("An existing connection was forcibly closed by the remote host")
            ) {
                result = true;
                detailedInfo = "An existing connection was forcibly closed by the remote host";
            }
            if(
                e.Message.Contains("Exception while reading from stream")
                && e.InnerException.Message == "Timeout during reading attempt"
            ) {
                result = true;
                detailedInfo = e.InnerException.Message;
            }
            if(
                e.Message == "Exception while writing to stream"
                && e.InnerException.Message == "Timeout during writing attempt"
            ) {
                result = true;
                detailedInfo = e.InnerException.Message;
            }

            if(result == true) {
                Console.WriteLine();
                MyConsole.Warning("Connection problem("+detailedInfo+"), retrying ...");
                System.Threading.Thread.Sleep(2000);
            }

            return result;
        }
    }
}
