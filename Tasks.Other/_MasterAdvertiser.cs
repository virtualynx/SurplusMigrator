using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _MasterAdvertiser : _BaseTask {
        public _MasterAdvertiser(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_advertiser",
                    columns = new string[] {
                        "code",
                        "name",
                        "cb_name",
                        "rpt_name",
                        "trf_code",
                        "active",
                        "entry_by",
                        "entry_dt",
                        "modify_by",
                        "modify_dt",
                    },
                    ids = new string[] { "code" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_advertiser",
                    columns = new string[] {
                        "advertiserid",
                        "name",
                        "cb_name",
                        "rpt_name",
                        "trf_code",
                        "created_by",
                        "created_date",
                        "is_disabled",
                        "disabled_by",
                        "disabled_date",
                        "modified_by",
                        "modified_date",
                    },
                    ids = new string[] { "advertiserid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_advertiser").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_advertiser",
                    new RowData<ColumnName, object>() {
                        { "advertiserid",  data["code"]},
                        { "name",  data["name"]},
                        { "cb_name",  data["cb_name"]},
                        { "rpt_name",  data["rpt_name"]},
                        { "trf_code",  data["trf_code"]},
                        { "created_by", getAuthInfo(data["entry_by"], true) },
                        { "created_date",  data["entry_dt"]},
                        { "is_disabled", !Utils.obj2bool(data["active"]) },
                        { "disabled_by",  null },
                        { "disabled_date",  null },
                        { "modified_by", getAuthInfo(data["modify_by"]) },
                        { "modified_date",  data["modify_dt"]},
                    }
                );
            }

            return result;
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_advertiser",
                new RowData<ColumnName, object>() {
                        { "advertiserid",  "131"},
                        { "name",  "{UNKNOWN}"},
                        { "cb_name",  "{UNKNOWN}"},
                        { "rpt_name",  "{UNKNOWN}"},
                        { "trf_code",  null},
                        { "created_by",  DefaultValues.CREATED_BY },
                        { "created_date",  DateTime.Now },
                        { "is_disabled", false },
                        { "disabled_by",  null },
                        { "disabled_date",  null },
                        { "modified_by",  null },
                        { "modified_date",  null },
                }
            );

            return result;
        }
    }
}
