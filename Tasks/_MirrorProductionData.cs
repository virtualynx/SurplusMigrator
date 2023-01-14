using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _MirrorProductionData : _BaseTask {
        private DbConnection_ sourceConnection;

        private const int DEFAULT_BATCH_SIZE = 200;

        private Dictionary<string, int> batchsizeMap = new Dictionary<string, int>() {
            { "transaction_budget", 500},
            { "transaction_budget_detail", 3500},
            { "transaction_journal", 3500 },
            { "transaction_journal_detail", 10000 },
            { "transaction_journal_tax", 1500},
            { "transaction_program_budget_eps_detail", 1500},
            { "transaction_sales_order", 1500}
        };

        private string[] excludedTables = new string[] {
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

        private string[] onlyMigrateTables = new string[] {
            
        };

        public _MirrorProductionData(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            sourceConnection = connections.Where(a => a.GetDbLoginInfo().name == "mirror_source").First();
        }

        protected override void afterFinishedCallback() {
            var tables = getTables();

            var tagetConnection = connections.Where(a => a.GetDbLoginInfo().name == "mirror_target").First();

            foreach(var row in tables) {
                try {
                    string tablename = row["table_name"].ToString();
                    var columns = QueryUtils.getColumnNames(sourceConnection, tablename);

                    MyConsole.Write("Deleting all data in table " + tablename + " ... ");
                    try {
                        QueryUtils.toggleTrigger(tagetConnection, tablename, false);
                        var rs = QueryUtils.executeQuery(tagetConnection, "DELETE FROM \""+ tagetConnection.GetDbLoginInfo().schema + "\".\""+ tablename + "\";");
                    } catch(Exception) {
                        throw;
                    } finally {
                        QueryUtils.toggleTrigger(tagetConnection, tablename, true);
                    }
                    MyConsole.WriteLine(" Done", false);

                    MyConsole.Information("Inserting into table " + tablename + " ... ");

                    var dataCount = QueryUtils.getDataCount(sourceConnection, tablename);
                    int insertedCount = 0;
                    int batchSize = batchsizeMap.ContainsKey(tablename)? batchsizeMap[tablename] : DEFAULT_BATCH_SIZE;

                    RowData<ColumnName, object>[] batchData;
                    bool firstLoop = true;
                    while((batchData = QueryUtils.getDataBatch(sourceConnection, tablename, false, batchSize)).Length > 0) {
                        try {
                            string query = @"
                                insert into ""[target_schema]"".""[tablename]""([columns])
                                values [values];
                            ";
                            query = query.Replace("[target_schema]", tagetConnection.GetDbLoginInfo().schema);
                            query = query.Replace("[tablename]", tablename);
                            query = query.Replace("[columns]", "\"" + String.Join("\",\"", columns) + "\"");

                            List<string> insertArgs = new List<string>();
                            foreach(var rowSource in batchData) {
                                List<string> valueArgs = new List<string>();
                                foreach(var map in rowSource) {
                                    string column = map.Key;
                                    object data = map.Value;
                                    valueArgs.Add(QueryUtils.getInsertArg(data));
                                }
                                insertArgs.Add("(" + String.Join(",", valueArgs) + ")");
                            }
                            query = query.Replace("[values]", String.Join(",", insertArgs));

                            QueryUtils.toggleTrigger(tagetConnection, tablename, false);
                            try {
                                var rs = QueryUtils.executeQuery(tagetConnection, query);
                            } catch(Exception e) {
                                if(!e.Message.Contains("duplicate key value violates unique constraint")) {
                                    MyConsole.Error(query);
                                }
                                throw;
                            } finally {
                                QueryUtils.toggleTrigger(tagetConnection, tablename, true);
                            }
                            insertedCount += batchData.Length;
                            if(firstLoop) {
                                firstLoop = false;
                            } else {
                                MyConsole.EraseLine();
                            }
                            MyConsole.Write(insertedCount + "/" + dataCount + " data inserted ... ");
                        } catch(Exception e) {
                            if(e.Message.Contains("duplicate key value violates unique constraint")) {
                                //MyConsole.Warning(e.Message);
                            } else {
                                throw;
                            }
                        }
                    }

                    QueryUtils.toggleTrigger(tagetConnection, tablename, true);
                    MyConsole.EraseLine();
                    MyConsole.Information("Successfully migrate "+ insertedCount + "/"+ dataCount + " data on table " + tablename);
                } catch(Exception) {
                    throw;
                }
            }
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

            var filtered = allTable.Where(a => !excludedTables.Any(b => Utils.obj2str(a["table_name"]) == b)).ToArray();

            if(onlyMigrateTables.Length > 0) {
                filtered = filtered.Where(a => onlyMigrateTables.Any(b => Utils.obj2str(a["table_name"]) == b)).ToArray();
            }

            return filtered;
        }
    }
}
