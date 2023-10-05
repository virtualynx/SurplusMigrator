using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class _MirrorSchema : _BaseTask {
        private DbConnection_ sourceConnection;
        private DbConnection_ targetConnection;

        private const int DEFAULT_BATCH_SIZE = 200;
        private bool _isModeTest = false;
        private bool _isModeNoQuery = false;
        private string _dotnetMigrationRepo = null;
        private string _dotnetMigrationProjectPath = null;
        private string _dotnetMigrationDbContext = null;

        private Dictionary<string, int> batchsizeMap = new Dictionary<string, int>() {
            { "transaction_budget", 1000},
            { "transaction_budget_detail", 3500},
            { "transaction_journal", 3500 },
            { "transaction_journal_detail", 10000 },
            { "transaction_journal_tax", 1500},
            { "transaction_program_budget_eps_detail", 1500},
            { "transaction_sales_order", 1500}
        };

        private List<string> excludedTables = new List<string> {
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

        public _MirrorSchema(DbConnection_[] connections) : base(connections) {
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

            if(getOptions("skips") != null) {
                string[] tableList = getOptions("skips").Split(",");
                foreach(var table in tableList) {
                    excludedTables.Add(table.Trim());
                }
            }

            if(getOptions("no-query") != null) {
                _isModeNoQuery = getOptions("no-query") == "true";
            }

            if(getOptions("dotnet-migration") == "true" && getOptions("dotnet-migration-repo") != null) {
                _dotnetMigrationRepo = getOptions("dotnet-migration-repo");
            }

            if(getOptions("dotnet-migration-project-path") != null) {
                _dotnetMigrationProjectPath = getOptions("dotnet-migration-project-path");
            }

            if(getOptions("dotnet-migration-db-context") != null) {
                _dotnetMigrationDbContext = getOptions("dotnet-migration-db-context");
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
            QueryExecutor qe = new QueryExecutor(connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault());

            if(_dotnetMigrationRepo != null) {
                string gitCloneCommand = "git clone "+_dotnetMigrationRepo;

                if(getOptions("dotnet-migration-repo-github-user-and-token") != null) {
                    string dotnetMigrationRepoGithubUserAndToken = getOptions("dotnet-migration-repo-github-user-and-token");
                    gitCloneCommand = gitCloneCommand.Replace("https://", "https://"+ dotnetMigrationRepoGithubUserAndToken + "@");
                }

                if(getOptions("dotnet-migration-repo-branch") != null) {
                    string dotnetMigrationRepoBranch = getOptions("dotnet-migration-repo-branch");
                    gitCloneCommand = gitCloneCommand.Replace("git clone", "git clone --single-branch --branch " + dotnetMigrationRepoBranch);
                }

                if(_dotnetMigrationProjectPath != null) {
                }
                if(_dotnetMigrationDbContext != null) { 
                
                }
            }

            if(!_isModeNoQuery) {
                qe.execute(GlobalConfig.getPreQueriesPath());
            }

            var tables = getTableNames();

            foreach(var tablename in tables) {
                NpgsqlTransaction transaction = ((NpgsqlConnection)targetConnection.GetDbConnection()).BeginTransaction();
                try {
                    var columns = QueryUtils.getColumnNames(sourceConnection, tablename);
                    var primaryKeys = QueryUtils.getPrimaryKeys(sourceConnection, tablename);

                    MyConsole.Write("Deleting all data in table " + tablename + " ... ");
                    try {
                        QueryUtils.toggleTrigger(targetConnection, tablename, false);
                        var rs = QueryUtils.executeQuery(targetConnection, "DELETE FROM \""+ targetConnection.GetDbLoginInfo().schema + "\".\""+ tablename + "\";", null, transaction, 0);
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
                        tablename = tablename,
                        columns = columns,
                        ids = primaryKeys,
                    };
                    Table targetTable = new Table() {
                        connection = targetConnection,
                        tablename = tablename,
                        columns = columns,
                        ids = primaryKeys,
                    };
                    List<RowData<ColumnName, object>> batchData;
                    while((batchData = sourceTable.getData(batchSize, null, true, false)).Count > 0) {
                        try {
                            try {
                                QueryUtils.toggleTrigger(targetConnection, tablename, false);
                                targetTable.insertData(batchData, transaction, false);
                                insertedCount += batchData.Count;
                                MyConsole.EraseLine();
                                MyConsole.Write(insertedCount + "/" + dataCount + " data inserted ... ");
                            } catch(Exception e) {
                                if(!e.Message.Contains("duplicate key value violates unique constraint")) {
                                    MyConsole.Error(e, e.Message);
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
                    targetTable.maximizeSequencerId();
                    transaction.Commit();
                    MyConsole.EraseLine();
                    MyConsole.Information("Successfully copying " + insertedCount + "/"+ dataCount + " data on table " + tablename);
                    MyConsole.WriteLine("", false);
                } catch(Exception) {
                    transaction.Rollback();
                    throw;
                }
            }

            if(!_isModeNoQuery) {
                qe.execute(GlobalConfig.getPostQueriesPath());
            }
        }

        private string[] getTableNames() {
            string query = @"
                SELECT 
	                table_name,
                    is_insertable_into
                FROM 
	                information_schema.tables 
                WHERE 
	                table_schema = @schema
	                and table_type = 'BASE TABLE'
                order by table_name 
                ;
            ";

            var sourceTables = QueryUtils.executeQuery(sourceConnection, query,
                new Dictionary<string, object> { { "@schema", sourceConnection.GetDbLoginInfo().schema } }
                );

            var targetTables = QueryUtils.executeQuery(targetConnection, query,
                new Dictionary<string, object> { { "@schema", targetConnection.GetDbLoginInfo().schema } }
                );

            var unionTable = targetTables.Where(tg => sourceTables.Any(sc => tg["table_name"].ToString() == sc["table_name"].ToString()))
                .Select(a => a["table_name"].ToString()).ToArray();

            var filtered = unionTable.Where(a => !excludedTables.Contains(a)).ToArray();

            if(onlyMigrateTables.Count > 0) {
                filtered = filtered.Where(a => onlyMigrateTables.Contains(a)).ToArray();
            }

            return filtered;
        }
    }
}
