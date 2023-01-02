using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Office.Interop.Excel;
using Serilog.Core;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Xml.Linq;

namespace SurplusMigrator.Tasks {
    class _MigrateProductionData : _BaseTask {
        public _MigrateProductionData(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        private DbConnection_ sourceConnection = new DbConnection_(
            new DbLoginInfo() {
                host = "172.16.123.121",
                port = 5432,
                username = "postgres",
                password = "initrans7",
                dbname = "insosys",
                schema = "_live",
                type = "POSTGRESQL"
            }    
        );

        private List<string> excludedTables = new List<string>() {
            "AspNetRoleClaims",
            "AspNetRoles",
            "AspNetUserClaims",
            "AspNetUserLogins",
            "AspNetUserRoles",
            "AspNetUserTokens",
            "__EFMigrationsHistory",
            "audit",
            "dataprotectionkeys"
        };

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            var tables = getTables();

            var tagetConnection = connections.Where(
                a =>
                    a.GetDbLoginInfo().dbname == "insosys"
                    && a.GetDbLoginInfo().type == "POSTGRESQL"
                    && a.GetDbLoginInfo().schema != "_live"
            ).FirstOrDefault();

            foreach(var row in tables) {
                try {
                    string tablename = row["table_name"].ToString();
                    var columns = QueryUtils.getColumnNames(sourceConnection, tablename);

                    if(!columns.Any(a => a == "created_by")) {
                        continue;
                    }
                    var sourceDatas = getSourceData(tablename);

                    if(sourceDatas.Length == 0) continue;

                    MyConsole.Information("Inserting into table " + tablename);

                    QueryUtils.executeQuery(tagetConnection, "ALTER TABLE \"" + tablename + "\" DISABLE TRIGGER ALL;");

                    int insertedCount = 0;
                    foreach(var rowSource in sourceDatas) {
                        try {
                            string query = @"
                                insert into ""[target_schema]"".""[tablename]""([columns])
                                values([values]);
                            ";
                            query = query.Replace("[target_schema]", tagetConnection.GetDbLoginInfo().schema);
                            query = query.Replace("[tablename]", tablename);
                            query = query.Replace("[columns]", "\""+String.Join("\",\"", columns) +"\"");
                            List<string> valueArgs = new List<string>();
                            foreach(var map in rowSource) {
                                string column = map.Key;
                                object data = map.Value;
                                string convertedData = null;
                                if(data == null) {
                                    convertedData = "NULL";
                                } else if(data.GetType() == typeof(string)) {
                                    convertedData = "'"+data.ToString()+"'";
                                } else if(data.GetType() == typeof(bool)) {
                                    convertedData = data.ToString().ToLower();
                                } else if(data.GetType() == typeof(DateTime)) {
                                    convertedData = "'"+((DateTime)data).ToString("yyyy-MM-dd HH:mm:ss.fff")+"'";
                                } else {
                                    convertedData = data.ToString();
                                }
                                valueArgs.Add(convertedData);
                            }
                            query = query.Replace("[values]", String.Join(",", valueArgs));
                            var rs = QueryUtils.executeQuery(tagetConnection, query);
                            insertedCount++;
                        } catch(Exception e) {
                            if(e.Message.Contains("duplicate key value violates unique constraint")) {
                                //MyConsole.Warning(e.Message);
                            } else {
                                throw;
                            }
                        }
                    }
                    QueryUtils.executeQuery(tagetConnection, "ALTER TABLE \"" + tablename + "\" ENABLE TRIGGER ALL;");
                    MyConsole.Information("Successfully migrate "+ insertedCount + " data on table " + tablename);
                } catch(Exception) {
                    throw;
                }
            }

            return new MappedData();
        }

        private RowData<ColumnName, object>[] getTables() {
            string query = @"
                SELECT 
	                table_name,
                    is_insertable_into
                FROM 
	                information_schema.tables 
                WHERE 
	                table_schema = '_staging'
	                and table_type = 'BASE TABLE'
                order by table_name 
                ;
            ";

            var allTable = QueryUtils.executeQuery(sourceConnection, query);

            return allTable.Where(a => !excludedTables.Any(b => Utils.obj2str(a["table_name"]) == b)).ToArray();
        }

        private RowData<ColumnName, object>[] getSourceData(string tablename) {
            var columns = QueryUtils.getColumnNames(sourceConnection, tablename);
            string query = @"
                select [columns] from ""[schema_name]"".""[tablename]""
                where 
                    created_by->> 'Id' is not null
                ;
            ";
            query = query.Replace("[columns]", "\"" + String.Join("\",\"", columns) + "\"");
            query = query.Replace("[schema_name]", sourceConnection.GetDbLoginInfo().schema);
            query = query.Replace("[tablename]", tablename);

            var rs = QueryUtils.executeQuery(sourceConnection, query);

            return rs.ToArray();
        }
    }
}
