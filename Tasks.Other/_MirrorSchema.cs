using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace SurplusMigrator.Tasks {
    class _MirrorSchema : _BaseTask {
        private DbConnection_ sourceConnection;
        private DbConnection_ targetConnection;

        private const int DEFAULT_BATCH_SIZE = 200;
        private bool _isModeTest = false;
        private bool _isModeNoQuery = false;

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

        private MigrationConfig migrationConfig = null;

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

            if(getOptions("dotnet-migration") == "true") {
                string migConfigStr;
                if(getOptions("dotnet-migration-config-file") != null) {
                    migConfigStr = File.ReadAllText(getOptions("dotnet-migration-config-file"));
                } else {
                    migConfigStr = File.ReadAllText("config.migration.json");
                }
                migrationConfig = JsonSerializer.Deserialize<MigrationConfig>(migConfigStr);
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

            if(migrationConfig != null) {
                string tempFolder = "temp";
                Utils.executeCmd("mkdir temp");

                string gitCloneCommand = "git clone "+migrationConfig.git_repo;

                if(migrationConfig.github_user_and_token != null) {
                    gitCloneCommand = gitCloneCommand.Replace("https://", "https://"+ migrationConfig.github_user_and_token + "@");
                }

                if(migrationConfig.git_branch != null) {
                    gitCloneCommand = gitCloneCommand.Replace("git clone", "git clone --single-branch --branch " + migrationConfig.git_branch);
                }

                string dotnetMigrationProjectPath = "";
                if(migrationConfig.path != null) {
                    dotnetMigrationProjectPath = migrationConfig.path;
                }

                //Utils.executeCmd("rmdir /s /q " + tempFolder + "/surplus");
                Utils.deleteDirectory(tempFolder + "/surplus");
                string res = Utils.executeCmd(gitCloneCommand, tempFolder, true);

                string appsettingStr = File.ReadAllText(tempFolder + "/" + dotnetMigrationProjectPath + "/appsettings.Development.json");
                var appsettingJson = JsonNode.Parse(appsettingStr);

                var dbUrlConnProperties = appsettingJson["ConnectionStrings"]["PostgresConnection"].ToString().Split(";");

                List<string> tempProps = dbUrlConnProperties.Where(a => !a.StartsWith("SearchPath=")).ToList();
                tempProps.Add("SearchPath="+ targetConnection.GetDbLoginInfo().schema);

                appsettingJson["ConnectionStrings"]["PostgresConnection"] = String.Join(";", tempProps);
                appsettingStr = appsettingJson.ToString();

                File.WriteAllText(tempFolder + "/" + dotnetMigrationProjectPath + "/appsettings.Development.json", appsettingStr);

                //create migration table
                try {
                    QueryUtils.executeQuery(
                        targetConnection,
                        @"
                            CREATE TABLE ""<schema>"".""__EFMigrationsHistory"" (
                                migrationid text NOT NULL,
                                productversion text NOT NULL,
                                CONSTRAINT ""PK_HistoryRow"" PRIMARY KEY(migrationid)
                            );
                        "
                        .Replace("<schema>", targetConnection.GetDbLoginInfo().schema)
                    );
                } catch(Exception e) {
                    MyConsole.Warning(e.Message);
                }

                string dotnetMigrationCommand = "dotnet ef database update";
                if(migrationConfig.db_context != null) {
                    dotnetMigrationCommand += " --context " + migrationConfig.db_context;
                }

                //res = Utils.executeCmd(new List<string>() { dotnetMigrationCommand }, dotnetMigrationProjectPath);
                res = Utils.executeCmd(dotnetMigrationCommand, tempFolder+"/"+dotnetMigrationProjectPath, true);
                MyConsole.WriteLine(res);
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

        private class MigrationConfig { 
            public string git_repo { get; set; }
            public string git_branch { get; set; }
            public string github_user_and_token { get; set; }
            public string path { get; set; }
            public string db_context { get; set; }
        }
    }
}
