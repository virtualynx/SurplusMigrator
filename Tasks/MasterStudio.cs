using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterStudio : _BaseTask {
        public MasterStudio(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "master_crewequipmentstudio",
                    columns = new string[] {
                        "studio_id",
                        "studio_name",
                        "studio_location",
                        "studio_remark",
                        "studio_category_id",
                        "studio_createdby",
                        "studio_createddate",
                        "studio_modifiedby",
                        "studio_modifieddate",
                        "studio_isdisabled",
                        "studio_disableby",
                        "studio_disabledate",
                    },
                    ids = new string[] { "studio_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_studio",
                    columns = new string[] {
                        "studioid",
                        "name",
                        "address",
                        "remark",
                        "studiogroupid",
                        "created_by",
                        "created_date",
                        "is_disabled",
                        "disabled_by",
                        "disabled_date",
                        "modified_by",
                        "modified_date",
                    },
                    ids = new string[] { "studioid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tablename == "master_crewequipmentstudio").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_studio",
                    new RowData<ColumnName, object>() {
                        { "studioid",  data["studio_id"]},
                        { "name",  data["studio_name"]},
                        { "address",  data["studio_location"]},
                        { "remark",  data["studio_remark"]},
                        { "studiogroupid",  data["studio_category_id"]},
                        { "created_by", getAuthInfo(data["studio_createdby"], true) },
                        { "created_date",  data["studio_createddate"]},
                        { "is_disabled", !Utils.obj2bool(data["studio_isdisabled"]) },
                        { "disabled_by",  getAuthInfo(data["studio_disableby"]) },
                        { "disabled_date",  data["studio_disabledate"] },
                        { "modified_by", getAuthInfo(data["studio_modifiedby"]) },
                        { "modified_date",  data["studio_modifieddate"]},
                    }
                );
            }

            return result;
        }
    }
}
