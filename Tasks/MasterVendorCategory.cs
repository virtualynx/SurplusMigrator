using SurplusMigrator.Models;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterVendorCategory : _BaseTask {
        public MasterVendorCategory(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_vendor_category",
                    columns = new string[] {
                        "vendorcategoryid",
                        "name",
                    },
                    ids = new string[] { "vendorcategoryid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();
            
            result.addData(
                "master_vendor_category",
                new RowData<ColumnName, object>() {
                    { "vendorcategoryid",  1},
                    { "name",  "Individual"},
                }
            );
            result.addData(
                "master_vendor_category",
                new RowData<ColumnName, object>() {
                    { "vendorcategoryid",  2},
                    { "name",  "Company"},
                }
            );

            return result;
        }
    }
}
