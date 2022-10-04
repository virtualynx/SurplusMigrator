using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterFaction : _BaseTask {
        public MasterFaction(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_golongan",
                    columns = new string[] {
                        "golongan_id",
                        "golongan_desc",
                    },
                    ids = new string[] { "golongan_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_faction",
                    columns = new string[] {
                        "factionid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled",
                    },
                    ids = new string[] { "factionid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            List<RowData<ColumnName, object>> result = new List<RowData<string, object>>();

            List<RowData<ColumnName, object>> masterGolongan = sourceTables.Where(a => a.tableName == "master_golongan").FirstOrDefault().getDatas(batchSize);
            Dictionary<string, string> factions = new Dictionary<string, string>();

            foreach(RowData<ColumnName, object> row in masterGolongan) {
                string[] golonganIdArr = row["golongan_id"].ToString().Split(" ");
                golonganIdArr = golonganIdArr.Where(a => a != "AA" && a != "AE" && a != "JP").ToArray();
                string golonganId = String.Join(' ', golonganIdArr);
                if(!factions.ContainsKey(golonganId)) {
                    factions[golonganId] = row["golongan_desc"].ToString();
                }
            }

            foreach(KeyValuePair<string, string> map in factions) {
                result.Add(
                    new RowData<string, object>() {
                        { "factionid",  map.Key},
                        { "name",  map.Value}
                    }    
                );
            }

            return result;
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                data["created_date"] = DateTime.Now;
                data["created_by"] = DefaultValues.CREATED_BY;
                data["is_disabled"] = false;
                result.addData("master_faction", data);
            }

            return result;
        }
    }
}
