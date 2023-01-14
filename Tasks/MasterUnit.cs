using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterUnit : _BaseTask {
        public MasterUnit(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_unit",
                    columns = new string[] {
                        "unit_id",
                        "unit_shortname",
                        "unit_name",
                        "unit_type",
                        "unit_base",
                        "unit_active"
                    },
                    ids = new string[] { "unit_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_unit",
                    columns = new string[] {
                        "unitid",
                        "name",
                        "shortname",
                        "type",
                        "base",
                        "created_date",
                        "created_by",
                        "disabled_date",
                        "is_disabled",
                    },
                    ids = new string[] { "unitid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_unit").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_unit",
                    new RowData<ColumnName, object>() {
                        { "unitid",  data["unit_id"]},
                        { "name",  data["unit_name"]},
                        { "shortname",  data["unit_shortname"]},
                        { "type",  data["unit_type"]},
                        { "base",  data["unit_base"]},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", !Utils.obj2bool(data["unit_active"]) }
                    }
                );
            }

            return result;
        }
    }
}
