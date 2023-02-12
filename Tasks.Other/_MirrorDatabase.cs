using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class _MirrorDatabase : _BaseTask {
        private DbConnection_ sourceConnection;
        private DbConnection_ targetConnection;

        private const int DEFAULT_BATCH_SIZE = 200;
        private bool _isModeTest = false;

        private Dictionary<string, int> batchsizeMap = new Dictionary<string, int>() {
            { "transaction_budget", 1000},
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

        private List<string> onlyMigrateTables = new List<string>() {
            
        };

        public _MirrorDatabase(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            if(getOptions("test") != null) {
                _isModeTest = true;
            }

            if(getOptions("tables") != null) {
                string[] tableList = getOptions("tables").Split(",");
                foreach(var table in tableList) {
                    onlyMigrateTables.Add(table.Trim());
                }
            }

            sourceConnection = connections.Where(a => a.GetDbLoginInfo().name == "mirror_source").First();
            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "mirror_target").First();
            Console.WriteLine("\n");
            MyConsole.Information("Mirror Source: " + JsonSerializer.Serialize(sourceConnection.GetDbLoginInfo()));
            Console.WriteLine();
            MyConsole.Information("Mirror Target: " + JsonSerializer.Serialize(targetConnection.GetDbLoginInfo()));
            Console.WriteLine();
            Console.Write("Continue performing database mirroring (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            var tables = getTables();

            foreach(var row in tables) {
                NpgsqlTransaction transaction = ((NpgsqlConnection)targetConnection.GetDbConnection()).BeginTransaction();
                try {
                    string tablename = row["table_name"].ToString();
                    var columns = QueryUtils.getColumnNames(sourceConnection, tablename);
                    var primaryKeys = QueryUtils.getPrimaryKeys(sourceConnection, tablename);

                    MyConsole.Write("Deleting all data in table " + tablename + " ... ");
                    try {
                        QueryUtils.toggleTrigger(targetConnection, tablename, false);
                        var rs = QueryUtils.executeQuery(targetConnection, "DELETE FROM \""+ targetConnection.GetDbLoginInfo().schema + "\".\""+ tablename + "\";", null, transaction);
                    } catch(Exception) {
                        throw;
                    } finally {
                        QueryUtils.toggleTrigger(targetConnection, tablename, true);
                    }
                    MyConsole.WriteLine(" Done", false);

                    MyConsole.Information("Inserting into table " + tablename + " ... ");

                    var dataCount = QueryUtils.getDataCount(sourceConnection, tablename);
                    int insertedCount = 0;
                    int batchSize = batchsizeMap.ContainsKey(tablename)? batchsizeMap[tablename] : DEFAULT_BATCH_SIZE;

                    Table sourceTable = new Table() {
                        connection = sourceConnection,
                        tableName = tablename,
                        columns = columns,
                        ids = primaryKeys,
                    };
                    Table targetTable = new Table() {
                        connection = targetConnection,
                        tableName = tablename,
                        columns = columns,
                        ids = primaryKeys,
                    };
                    List<RowData<ColumnName, object>> batchData;
                    while((batchData = sourceTable.getDatas(batchSize, null, true, false)).Count > 0) {
                        try {
                            try {
                                QueryUtils.toggleTrigger(targetConnection, tablename, false);
                                targetTable.insertData(batchData, false, false, transaction, false);
                                insertedCount += batchData.Count;
                                MyConsole.EraseLine();
                                MyConsole.Write(insertedCount + "/" + dataCount + " data inserted ... ");
                            } catch(Exception e) {
                                if(!e.Message.Contains("duplicate key value violates unique constraint")) {
                                    throw;
                                }
                            } finally {
                                QueryUtils.toggleTrigger(targetConnection, tablename, true);
                            }
                        } catch(Exception e) {
                            if(e.Message.Contains("duplicate key value violates unique constraint")) {
                                //MyConsole.Warning(e.Message);
                            } else {
                                throw;
                            }
                        }

                        if(_isModeTest) break;
                    }

                    //update sequencer
                    targetTable.updateSequencer();
                    transaction.Commit();
                    MyConsole.EraseLine();
                    MyConsole.Information("Successfully copying " + insertedCount + "/"+ dataCount + " data on table " + tablename);
                    MyConsole.WriteLine("", false);
                } catch(Exception) {
                    transaction.Rollback();
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
	                table_schema = '<schema>'
	                and table_type = 'BASE TABLE'
                order by table_name 
                ;
            ";

            query = query.Replace("<schema>", sourceConnection.GetDbLoginInfo().schema);

            var allTable = QueryUtils.executeQuery(sourceConnection, query);

            var filtered = allTable.Where(a => !excludedTables.Any(b => Utils.obj2str(a["table_name"]) == b)).ToArray();

            if(onlyMigrateTables.Count > 0) {
                filtered = filtered.Where(a => onlyMigrateTables.Any(b => Utils.obj2str(a["table_name"]) == b)).ToArray();
            }

            return filtered;
        }
    }
}
