using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixJournalEmptyVendor : _BaseTask {
        private DbConnection_ conn;

        public _FixJournalEmptyVendor(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            conn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
        }

        protected override void onFinished() {
            var sourcedata = getDataFromExcel();

            var vendorNames = sourcedata.Select(a => Utils.obj2str(a["Vendor Name"])).Distinct().ToArray();

            var vendorNameIdMap = getVendorNameIdMap(vendorNames);

            foreach(var name in vendorNames) {
                var tjournalids = sourcedata.Where(a => Utils.obj2str(a["Vendor Name"]) == name).Select(a => Utils.obj2str(a["ID"])).ToArray();

                QueryUtils.executeQuery(
                    conn,
                    @"update transaction_journal set vendorid = @vendorid where tjournalid in @tjournalids",
                    new Dictionary<string, object> {
                        { "@vendorid", vendorNameIdMap[name] },
                        { "@tjournalids", tjournalids }
                    },
                    null,
                    300
                );

                MyConsole.Information(
                    "Set vendorid = @vendorid in transaction_journal (@count data)"
                    .Replace("@vendorid", vendorNameIdMap[name].ToString())
                    .Replace("@count", tjournalids.Length.ToString())
                );
            }
        }

        private RowData<ColumnName, object>[] getDataFromExcel() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="Vendor Name", ordinal=3 },
                new ExcelColumn(){ name="ID", ordinal=4 },
            };

            var excelData = Utils.getDataFromExcel("Aging Detail_Feb_2023_MappingVendor_Unknown.xlsx", columns).ToArray();

            return excelData.Where(a => Utils.obj2str(a["Vendor Name"]) != null && a["Vendor Name"].ToString() != "Vendor Name").ToArray();
        }

        private Dictionary<string, int> getVendorNameIdMap(string[] names) {
            var result = new Dictionary<string, int>();

            int batchSize = 50;
            for(int a=0; a<names.Length; a+=batchSize) {
                string[] batchNames = names.Skip(a).Take(batchSize).ToArray();

                var rs = QueryUtils.executeQuery(
                    conn,
                    "select vendorid, name from master_vendor where name in @names",
                    new Dictionary<string, object>{ { "@names", batchNames } }
                );

                foreach(var row in rs) {
                    int vendorid = Utils.obj2int(row["vendorid"]);
                    string name = Utils.obj2str(row["name"]);

                    if(!result.ContainsKey(name)) {
                        result[name] = vendorid;
                    }
                }
            }

            return result;
        }
    }
}
