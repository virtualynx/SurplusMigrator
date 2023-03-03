using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterTrafficAgency : _BaseTask {
        private static int advertiserIdCounter = 1;

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
                        "trafficagencyid",
                        "vendorid",
                        "name",
                        "created_by",
                        "created_date",
                        "is_disabled",
                    },
                    ids = new string[] { "vendorid", "name" }
                }
            };

            var res = QueryUtils.searchSimilar(
                connections.Where(a => a.GetDbLoginInfo().name == "surplus").First(), 
                "master_vendor",
                new string[] { "vendorid", "name" },
                "name",
                "Leo Burnett Kreasindo Indonesia, PT"
            );

            var res2 = QueryUtils.searchSimilar(
                connections.Where(a => a.GetDbLoginInfo().name == "gen21").First(),
                "view_master_advertiser_temp",
                new string[] { "advertiserid" },
                "name",
                "Konidin"
            );

            var a = 1;
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_trafficagency").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_traffic_agency",
                    new RowData<ColumnName, object>() {
                        { "trafficagencyid",  advertiserIdCounter++},
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
