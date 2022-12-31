using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using Npgsql;

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
    }
}
