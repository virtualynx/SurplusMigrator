using Microsoft.EntityFrameworkCore.Metadata.Internal;
using SurplusMigrator.Exceptions.Gen21;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _ChangeBrand : _BaseTask {
        private DbConnection_ targetConnection;

        private const int DEFAULT_BATCH_SIZE = 500;

        private string[] tableToFix = new string[] {
            "transaction_journal",
            "transaction_sales_order"
        };

        public _ChangeBrand(DbConnection_[] connections) : base(connections) {
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

            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            Console.WriteLine("\n");
            MyConsole.Information("Brand will be changed on table (schema " + targetConnection.GetDbLoginInfo().schema + "): " + String.Join(",", tableToFix));
            Console.WriteLine();
            Console.Write("Continue performing fix (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            var missingBrands = getDataFromExcel();

            missingBrands = missingBrands.Skip(2).ToArray(); //removes headers
            missingBrands = missingBrands.Where(a => Utils.obj2str(a["id brand"]) != null).ToArray(); //removes empty-map

            Dictionary<string, List<string>> missingBrandGrouped = new Dictionary<string, List<string>>();
            foreach(var brand in missingBrands) {
                string insosysBrandid = Utils.obj2str(brand["brandid"]);
                string gen21brandid = Utils.obj2str(brand["id brand"]);
                if(!missingBrandGrouped.ContainsKey(gen21brandid)) {
                    missingBrandGrouped[gen21brandid] = new List<string>();
                }

                if(!missingBrandGrouped[gen21brandid].Contains(insosysBrandid)) {
                    missingBrandGrouped[gen21brandid].Add(insosysBrandid);
                }
            }

            foreach(var groupedBrand in missingBrandGrouped) {
                string gen21brandid = groupedBrand.Key;
                var listFrom = groupedBrand.Value.ToArray();

                try {
                    MyConsole.Information(
                        "Change insosys-brandid [<list_from>] to gen21-brandid [<gen21_id>] ... "
                        .Replace("<list_from>", String.Join(", ", listFrom))
                        .Replace("<gen21_id>", gen21brandid)
                    );

                    foreach(string tablename in tableToFix) {
                        string queryUpdate = @"
                                update ""<tablename>"" 
                                set advertiserbrandid = @newbrandid
                                where advertiserbrandid IN @list_from;
                            "
                            .Replace("<tablename>", tablename)
                        ;

                        var updateResult = QueryUtils.executeQuery(
                            targetConnection,
                            queryUpdate,
                            new Dictionary<string, object> {
                                { "@newbrandid", gen21brandid },
                                { "@list_from", listFrom }
                            }
                        );

                        MyConsole.WriteLine("Table "+ tablename+ " updated ...");
                    }
                } catch(Exception) {
                    throw;
                }
            }
        }

        private RowData<ColumnName, object>[] getDataFromExcel() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="brandid", ordinal=0 },
                new ExcelColumn(){ name="id brand", ordinal=3 },
            };

            return Utils.getDataFromExcel("missing_brands_Compare.xlsx", columns).ToArray();
        }
    }
}
