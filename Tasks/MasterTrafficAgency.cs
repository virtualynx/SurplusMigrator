using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterTrafficAgency : _BaseTask {
        public MasterTrafficAgency(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_trafficagency",
                    columns = new string[] {
                        "rekanan_id",
                        "trafficagency_line",
                        "trafficagency_name",
                        "trafficagency_isactive",
                    },
                    ids = new string[] { "rekanan_id", "trafficagency_line" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_traffic_agency",
                    columns = new string[] {
                        //"trafficagencyid",
                        "vendorid",
                        "name",
                        "created_by",
                        "created_date",
                        "is_disabled",
                    },
                    ids = new string[] { "vendorid", "name" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_trafficagency").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            var insosysConn = connections.First(a => a.GetDbLoginInfo().name == "e_frm");
            skipsIfMissingReferences("rekanan_id", "master_rekanan", "rekanan_id", insosysConn, inputs);

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_traffic_agency",
                    new RowData<ColumnName, object>() {
                        //{ "trafficagencyid",  trafficAgencyLastId++},
                        { "vendorid",  data["rekanan_id"]},
                        { "name",  data["trafficagency_name"]},
                        { "created_by", DefaultValues.CREATED_BY},
                        { "created_date", DateTime.Now},
                        { "is_disabled", !Utils.obj2bool(data["trafficagency_isactive"])},
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterVendor(connections).run();
        }
    }
}
