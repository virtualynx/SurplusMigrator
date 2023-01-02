using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using Npgsql;
using Microsoft.Data.SqlClient;

namespace SurplusMigrator.Libraries {
    class QueryUtils {
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
                      pg_class.oid = '[tablename]'::regclass AND 
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
                throw new NotImplementedException();
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
