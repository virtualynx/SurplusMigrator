using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterProcurementCategory : _BaseTask {
        public MasterProcurementCategory(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_procurementcategory",
                    columns = new string[] {
                        "procurementcategory_id",
                        "procurementcategory_name",
                        "objective_id",
                        "procurementcategory_isdisabled",
                        "procurementcategory_disabledby",
                        "procurementcategory_disableddate",
                        "procurementcategory_createdby",
                        "procurementcategory_createddate",
                        "procurementcategory_modifiedby",
                        "procurementcategory_modifieddate"
                    },
                    ids = new string[] { "procurementcategory_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_procurement_category",
                    columns = new string[] {
                        "procurementcategoryid",
                        "name",
                        "objective_id",
                        "created_date",
                        "created_by",
                        "disabled_date",
                        "is_disabled",
                        "disabled_by",
                        "modified_date",
                        "modified_by"
                    },
                    ids = new string[] { "procurementcategoryid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_procurementcategory").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_procurement_category",
                    new RowData<ColumnName, object>() {
                        { "procurementcategoryid",  data["procurementcategory_id"]},
                        { "name",  data["procurementcategory_name"]},
                        { "objective_id",  data["procurementcategory_name"]},
                        { "created_date", Utils.obj2datetimeNullable(data["procurementcategory_createddate"])},
                        { "created_by", getAuthInfo(data["procurementcategory_createdby"], true)},
                        { "is_disabled", Utils.obj2bool(data["procurementcategory_isdisabled"]) },
                        { "disabled_date", Utils.obj2datetimeNullable(data["procurementcategory_disableddate"])},
                        { "disabled_by", getAuthInfo(data["procurementcategory_disabledby"])},
                        { "modified_date", Utils.obj2datetimeNullable(data["procurementcategory_modifieddate"])},
                        { "modified_by", getAuthInfo(data["procurementcategory_modifiedby"])}
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterObjective(connections).run();
        }
    }
}
