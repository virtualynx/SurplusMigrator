using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class RelationFactionPosition : _BaseTask {
        public RelationFactionPosition(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "relasiGol_Jabatan",
                    columns = new string[] {
                        "relasi_id",
                        "golongan_id",
                        "Jabatan",
                    },
                    ids = new string[] { "relasi_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "relation_faction_position",
                    columns = new string[] {
                        "factionid",
                        "position",
                    },
                    ids = new string[] { "factionid", "position" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "relasiGol_Jabatan").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "relation_faction_position",
                    new RowData<ColumnName, object>() {
                        { "factionid",  data["golongan_id"]},
                        { "position",  data["Jabatan"]},
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterFaction(connections).run();
        }
    }
}
