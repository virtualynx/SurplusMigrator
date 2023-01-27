using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixAdvertiserBrand : _BaseTask {
        private DbConnection_ targetConnection;
        private Gen21Integration gen21;

        private const int DEFAULT_BATCH_SIZE = 500;

        private string[] tableToFix = new string[] {
            "transaction_journal",
            "transaction_sales_order"
        };

        public _FixAdvertiserBrand(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            if(getOptions("tables") != null) {
                string[] tableList = getOptions("tables").Split(",");
                if(tableList.Length > 0) {
                    tableToFix = (from table in tableList select table.Trim()).ToArray();
                }
            }

            gen21 = new Gen21Integration(connections);
            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            Console.WriteLine("\n");
            MyConsole.Information("Advertiser and Brand will be fixed on table (schema "+targetConnection.GetDbLoginInfo().schema+"): " + String.Join(",", tableToFix));
            Console.WriteLine();
            Console.Write("Continue performing fix (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            foreach(var tablename in tableToFix) {
                try {
                    MyConsole.Information("Fixing advertiserid and advertiserbrandid in " + tablename + " ... ");

                    string[] primaryKeys = QueryUtils.getPrimaryKeys(targetConnection, tablename);
                    int updatedCount = 0;

                    var distinctedBrand = QueryUtils.executeQuery(targetConnection, "select distinct(advertiserbrandid) from transaction_journal where advertiserbrandid is not null");

                    foreach(var dbrow in distinctedBrand) {
                        string advertiserbrandid = dbrow["advertiserbrandid"].ToString();

                        string queryCount = @"
                                select count(1) from ""<target_schema>"".""<tablename>""
                                where ""advertiserbrandid"" = <oldvalue>
                                ;
                            ";
                        queryCount = queryCount.Replace("<target_schema>", targetConnection.GetDbLoginInfo().schema);
                        queryCount = queryCount.Replace("<tablename>", tablename);
                        queryCount = queryCount.Replace("<oldvalue>", QueryUtils.getInsertArg(advertiserbrandid));
                        var rsCount = QueryUtils.executeQuery(targetConnection, queryCount);
                        int dataCount = Utils.obj2int(rsCount.First()["count"]);

                        string querySelect = @"
                                select <select_columns> from ""<target_schema>"".""<tablename>""
                                where 
                                    advertiserid is not null
                                    and advertiserbrandid = <oldvalue>
                                LIMIT <batch_size>
                                ;
                            ";
                        List<string> selectColumns = new List<string>() { "advertiserid" };
                        selectColumns.AddRange(primaryKeys);
                        querySelect = querySelect.Replace("<select_columns>", "\"" + String.Join("\",\"", selectColumns) + "\"");
                        querySelect = querySelect.Replace("<target_schema>", targetConnection.GetDbLoginInfo().schema);
                        querySelect = querySelect.Replace("<tablename>", tablename);
                        querySelect = querySelect.Replace("<oldvalue>", QueryUtils.getInsertArg(advertiserbrandid));
                        querySelect = querySelect.Replace("<batch_size>", DEFAULT_BATCH_SIZE.ToString());

                        int processedCount = 0;
                        RowData<ColumnName, object>[] batchDatas;
                        while((batchDatas = QueryUtils.executeQuery(targetConnection, querySelect)).Length > 0) {
                            var distinctedAdvertiserId = (from row in batchDatas select row["advertiserid"].ToString()).Distinct().ToArray();
                            string advertiserid = dbrow["advertiserid"].ToString();
                            (string newAdvertiserId, string newBrandId) = gen21.getAdvertiserBrandId(distinctedAdvertiserId[0], advertiserbrandid);

                            string queryUpdate = @"
                                    update ""<target_schema>"".""<tablename>"" 
                                    set 
                                        advertiserbrandid = <newbrandid>
                                    where (<filter_columns>) IN (<filter_values>);
                                ";
                            queryUpdate = queryUpdate.Replace("<target_schema>", targetConnection.GetDbLoginInfo().schema);
                            queryUpdate = queryUpdate.Replace("<tablename>", tablename);
                            queryUpdate = queryUpdate.Replace("<newbrandid>", QueryUtils.getInsertArg(newDeptId));
                            queryUpdate = queryUpdate.Replace("<filter_columns>", "\"" + String.Join("\",\"", primaryKeys) + "\"");

                            List<string> filterValues = new List<string>();
                            foreach(var row in batchDatas) {
                                List<string> filterValArray = new List<string>();
                                foreach(var pk in primaryKeys) {
                                    filterValArray.Add(QueryUtils.getInsertArg(row[pk]));
                                }
                                filterValues.Add("(" + String.Join(",", filterValArray) + ")");
                            }
                            queryUpdate = queryUpdate.Replace("<filter_values>", String.Join(",", filterValues));
                            QueryUtils.executeQuery(targetConnection, queryUpdate);
                            processedCount += batchDatas.Length;
                            MyConsole.EraseLine();
                            MyConsole.Write(processedCount + "/" + dataCount + " data processed ... ");
                        }
                    }

                    MyConsole.EraseLine();
                    MyConsole.Information("Successfully copying " + updatedCount + "/"+ dataCount + " data on table " + tablename);
                    MyConsole.WriteLine("", false);
                } catch(Exception) {
                    throw;
                }
            }
        }
    }
}
