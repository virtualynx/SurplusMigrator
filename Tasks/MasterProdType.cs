using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterProdType : _BaseTask {
        public MasterProdType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_prodtype",
                    columns = new string[] {
                        "prodtype_id",
                        "prodtype_name",
                        "prodtype_nameshort",
                        "prodtype_isactive",
                        "prodtype_entry_by",
                        "prodtype_entry_dt",
                    },
                    ids = new string[] { "prodtype_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_prod_type",
                    columns = new string[] {
                        "prodtypeid",
                        "name",
                        "nameshort",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "prodtypeid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_prodtype").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "prodtypeid",  data["prodtype_id"]},
                    { "name",  data["prodtype_name"]},
                    { "nameshort",  data["prodtype_nameshort"]},
                    { "created_date",  data["prodtype_entry_dt"]},
                    { "created_by",  new AuthInfo(){ FullName = Utils.obj2str(data["prodtype_entry_by"]) } },
                    { "is_disabled", !Utils.obj2bool(data["prodtype_isactive"]) }
                };
                result.addData("master_prod_type", insertRow);
            }

            return result;
        }
    }
}
