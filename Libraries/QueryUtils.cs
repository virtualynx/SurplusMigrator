using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using Npgsql;
using Microsoft.Data.SqlClient;
using System.Linq;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

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
                    RowData<ColumnName, dynamic> rowData = new RowData<ColumnName, dynamic>();
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
                    RowData<ColumnName, dynamic> rowData = new RowData<ColumnName, dynamic>();
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
                    RowData<ColumnName, dynamic> rowData = new RowData<ColumnName, dynamic>();
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

        public static RowData<ColumnName, object>[] executeQuery(DbConnection_ connection, string sql) {
            List<RowData<ColumnName, object>> result = new List<RowData<string, dynamic>>();
            bool retry = false;
            do {
                try {
                    if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                        SqlCommand command = new SqlCommand(sql, (SqlConnection)connection.GetDbConnection());
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
                        command.Dispose();
                    } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                        NpgsqlCommand command = new NpgsqlCommand(sql, (NpgsqlConnection)connection.GetDbConnection());
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
                        command.Dispose();
                    }
                    retry = false;
                } catch(Exception e) {
                    if(isConnectionProblem(e)) {
                        retry = true;
                    } else {
                        throw;
                    }
                }
            } while(retry);

            return result.ToArray();
        }

        public static int getDataCount(DbConnection_ connection, string tablename) {
            char[] dbObjectEnclosers = new char[] { };
            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                dbObjectEnclosers = new char[] { '[', ']' };
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                dbObjectEnclosers = new char[] { '"', '"' };
            }

            string queryCount = "select count(1) as datacount from " + dbObjectEnclosers[0] + connection.GetDbLoginInfo().schema + dbObjectEnclosers[1] + "." + dbObjectEnclosers[0] + tablename + dbObjectEnclosers[1]; ;
            var count = executeQuery(connection, queryCount);

            return Utils.obj2int(count[0]["datacount"]);
        }

        public static RowData<ColumnName, object>[] getDataBatch(
            DbConnection_ connection,
            string tablename,
            bool onlyApplicationGeneratedData = true,
            int batchSize = 10000,
            string[] ids = null
        ) {
            var batchInfo = batchInfos.Where(a => 
                a.dbLoginInfo == connection.GetDbLoginInfo() 
                && a.tablename == tablename
            ).FirstOrDefault();

            if(batchInfo == null) {
                batchInfo = new BatchInfo() {
                    dbLoginInfo = connection.GetDbLoginInfo(),
                    tablename = tablename,
                    dataCount = getDataCount(connection, tablename),
                    dataRead = 0
                };
                batchInfos.Add(batchInfo);
            } else if(batchInfo.dataRead >= batchInfo.dataCount) {
                batchInfo.dataCount = getDataCount(connection, tablename);
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
                            ) AS RowConstrainedResult
                    WHERE   
                        RowNum >= <offset_start>
                        AND RowNum <= <offset_end>
                    ORDER BY RowNum";

                query = query.Replace("<selected_columns>", String.Join(',', columns));
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
                ";
                if(onlyApplicationGeneratedData) {
                    query += " where created_by is null or created_by->> 'Id' is not null";
                }
                string orderBy = " order by ";
                if(ids.Length > 0) {
                    orderBy += "\"" + String.Join("\",\"", ids) + "\"";
                } else {
                    orderBy += "\"" + String.Join("\",\"", columns) + "\"";
                }
                query += orderBy;
                query = query.Replace("<columns>", "\"" + String.Join("\",\"", columns) + "\"");
                query = query.Replace("<schema_name>", connection.GetDbLoginInfo().schema);
                query = query.Replace("<tablename>", tablename);
                query += " offset " + batchInfo.dataRead + " limit " + batchSize;
            } else {
                throw new NotImplementedException("Unknown database implementation");
            }

            int readUntil = batchInfo.dataRead + batchSize;
            readUntil = readUntil > batchInfo.dataCount ? batchInfo.dataCount : readUntil;
            MyConsole.WriteLine("Read batch data " + tablename + " " + batchInfo.dataRead + " - " + readUntil + " / " + batchInfo.dataCount);
            var rs = executeQuery(connection, query);

            batchInfo.dataRead = readUntil;

            return rs.ToArray();
        }

        public static void toggleTrigger(DbConnection_ connection, string tablename, bool enable) {
            string enableStr = enable ? "ENABLE" : "DISABLE";
            executeQuery(connection, "ALTER TABLE \""+ connection .GetDbLoginInfo().schema+ "\".\"" + tablename + "\" "+ enableStr + " TRIGGER ALL;");
        }

        public static bool isConnectionProblem(Exception e) {
            bool result = false;

            if(
                e.Message.Contains("The timeout period elapsed prior to completion of the operation or the server is not responding")
                && e.InnerException.Message == "The wait operation timed out."
            ) {
                result = true;
            }
            if(
                e.Message.Contains("A network-related or instance-specific error occurred while establishing a connection to SQL Server")
                && e.Message.Contains("established connection failed because connected host has failed to respond")
            ) {
                result = true;
            }
            if(
                e.Message.Contains("A transport-level error has occurred when sending the request to the server")
                && e.Message.Contains("An existing connection was forcibly closed by the remote host")
            ) {
                result = true;
            }
            if(
                e.Message.Contains("Exception while reading from stream")
                && e.InnerException.Message == "Timeout during reading attempt"
            ) {
                result = true;
            }
            if(
                e.Message == "Exception while writing to stream"
                && e.InnerException.Message == "Timeout during writing attempt"
            ) {
                result = true;
            }

            if(result == true) {
                MyConsole.Warning("Connection problem, retrying ...");
                System.Threading.Thread.Sleep(2000);
            }

            return result;
        }
    }
}
