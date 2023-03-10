using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class _MirrorSchema_v2 : _BaseTask {
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

        public _MirrorSchema_v2(DbConnection_[] connections) : base(connections) {
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
            var tables = getTableNames();

            foreach(var tablename in tables) {
                NpgsqlTransaction transaction = ((NpgsqlConnection)targetConnection.GetDbConnection()).BeginTransaction();
                try {
                    QueryUtils.toggleTrigger(targetConnection, tablename, false);

                    MyConsole.Write("Deleting all data in table " + tablename + " ... ");
                    var rs = QueryUtils.executeQuery(targetConnection, "DELETE FROM \"" + targetConnection.GetDbLoginInfo().schema + "\".\"" + tablename + "\";", null, transaction);
                    MyConsole.WriteLine(" Done", false);

                    MyConsole.Information("Inserting into table " + tablename + " ... ");

                    string queryInsert = @"
                        insert into ""<target_schema>"".""<tablename>"" 
                        select * from ""<source_schema>"".""<tablename>""
                    "
                    .Replace("<target_schema>", targetConnection.GetDbLoginInfo().schema)
                    .Replace("<source_schema>", sourceConnection.GetDbLoginInfo().schema)
                    .Replace("<tablename>", tablename)
                    ;

                    QueryUtils.executeQuery(sourceConnection, queryInsert, null, transaction, 60*10);

                    var dataCount = QueryUtils.getDataCount(sourceConnection, tablename);
                    int insertedCount = QueryUtils.getDataCount(targetConnection, tablename);

                    Table targetTable = new Table() {
                        connection = targetConnection,
                        tablename = tablename,
                        columns = QueryUtils.getColumnNames(targetConnection, tablename),
                        ids = QueryUtils.getPrimaryKeys(targetConnection, tablename),
                    };

                    //update sequencer
                    targetTable.maximizeSequencerId();
                    transaction.Commit();
                    MyConsole.EraseLine();
                    MyConsole.Information("Successfully copying " + insertedCount + "/" + dataCount + " data on table " + tablename);
                    MyConsole.WriteLine("", false);
                } catch(Exception) {
                    transaction.Rollback();
                    throw;
                } finally {
                    QueryUtils.toggleTrigger(targetConnection, tablename, true);
                }
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
