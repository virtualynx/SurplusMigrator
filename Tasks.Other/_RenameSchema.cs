using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _RenameSchema : _BaseTask {
        private DbConnection_ _connection;

        private List<string> excludedFiles = new List<string> {
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

        public _RenameSchema(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            //if(getOptions("tables") != null) {k0kg1tu
            //    string[] tableList = getOptions("tables").Split(",");
            //    foreach(var table in tableList) {
            //        onlyMigrateTables.Add(table.Trim());
            //    }
            //} 

            _connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            Console.Write("Continue performing rename schema \""+ _connection.GetDbLoginInfo().schema + "\" -> \""+ getOptions("new_name") + "\" (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            string functionDir = Directory.GetDirectories(GlobalConfig.getPreQueriesPath(), "*_functions").First();
            string[] files = Directory.GetFiles(functionDir, "*.sql", SearchOption.AllDirectories);

            files = files.Where(
                a => {
                    string sql = File.ReadAllText(a);
                    return sql.Contains("<schema>");
                }
            ).ToArray();

            Array.Sort(files);

            var transaction = _connection.GetDbConnection().BeginTransaction();
            try {
                //dropFunctions(files, transaction);
                //recreateFunctions(files, getOptions("new_name"), transaction);

                QueryUtils.executeQuery(
                    _connection,
                    @"ALTER SCHEMA @schema_name RENAME TO @new_name"
                    .Replace("@schema_name", _connection.GetDbLoginInfo().schema)
                    .Replace("@new_name", getOptions("new_name")),
                    null,
                    transaction
                );

                transaction.Commit();

                DbLoginInfo loginInfo = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault().GetDbLoginInfo();
                loginInfo.schema = getOptions("new_name");

                DbConnection_ dbConn = new DbConnection_(loginInfo);

                QueryExecutor qe = new QueryExecutor(dbConn);
                qe.execute(GlobalConfig.getPreQueriesPath()+"/03_functions");
            } catch (Exception e) {
                transaction.Rollback();
                throw;
            }
        }

        private void dropFunctions(string[] files, DbTransaction transaction) {
            List<string> functionNames = new List<string> ();
            foreach(string filename in files) {
                FileAttributes attr = File.GetAttributes(filename);
                if((attr & FileAttributes.Directory) != FileAttributes.Directory) {
                    string[] sqlSplitted = File.ReadAllText(filename).Split(" ");
                    sqlSplitted = sqlSplitted.Where(a => a != "" && a != null).ToArray();
                    string functionName = null;
                    for(int a = 0; a < sqlSplitted.Length; a++) {
                        if(sqlSplitted[a].ToLower() == "procedure" || sqlSplitted[a].ToLower() == "function") {
                            if(sqlSplitted[a + 2] == "(") {
                                functionName = sqlSplitted[a + 1];
                                break;
                            } else if(sqlSplitted[a + 1].Split("(").Length == 2) {
                                functionName = sqlSplitted[a + 1].Split("(")[0];
                                break;
                            }
                        }
                    }

                    if(functionName == null) {
                        throw new Exception("No function/procedure name found in file \""+ filename + "\"");
                    }
                    functionNames.Add(functionName);
                }
            }

            try {
                foreach(string functionName in functionNames) {
                    QueryUtils.executeQuery(
                        _connection,
                        @"DROP ROUTINE IF EXISTS @name CASCADE;".Replace("@name", functionName),
                        null,
                        transaction
                    );
                }
            } catch (Exception e) {
                throw;
            }
        }

        private void recreateFunctions(string[] files, string newSchemaName, DbTransaction transaction) {
            foreach(string file in files) {
                string sql = File.ReadAllText(file);
                DateTime startAt = DateTime.Now;
                TimeSpan elapsed;
                MyConsole.Write("Executing " + file + " ... ");
                try {
                    sql = sql.Replace("<schema>", "\"" + newSchemaName + "\"");
                    QueryUtils.executeQuery(_connection, sql, null, transaction);

                    elapsed = DateTime.Now - startAt;
                    MyConsole.WriteLine("Success(" + elapsed + ").", false);
                } catch(PostgresException e) {
                    elapsed = DateTime.Now - startAt;
                    MyConsole.WriteLine("", false);
                    if(e.Message.Contains("duplicate key value violates unique constraint") && e.Message.Contains("already exists")) {
                        MyConsole.Warning("Skipping file " + file + ", is already executed.");
                    } else {
                        MyConsole.Error("Error(" + elapsed + "): " + e.Message, false);
                        throw;
                    }
                } catch(Exception e) {
                    elapsed = DateTime.Now - startAt;
                    MyConsole.WriteLine("", false);
                    MyConsole.Error(e, "Error(" + elapsed + "): " + e.Message, false);
                    throw;
                }
            }
        }
    }
}
