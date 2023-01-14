using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterFactionZoneRate : _BaseTask {
        public MasterFactionZoneRate(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_golongan",
                    columns = new string[] {
                        "golongan_id",
                        "golongan_desc",
                        "golongan_tipe",
                        "golongan_perdiems",
                        "golongan_uangsaku",
                        "golongan_uangmakan",
                        "golongan_uanglaundry",
                    },
                    ids = new string[] { "golongan_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_faction_zone_rate",
                    columns = new string[] {
                        "factionid",
                        "zoneid",
                        "currencyid",
                        "perdiemsmoney",
                        "pocketmoney",
                        "mealmoney",
                        "laundrymoney",
                    },
                    ids = new string[] { "factionid", "zoneid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            List<RowData<ColumnName, object>> result = new List<RowData<string, object>>();

            List<RowData<ColumnName, object>> masterGolongan = sourceTables.Where(a => a.tableName == "master_golongan").FirstOrDefault().getDatas(batchSize);
            
            foreach(RowData<ColumnName, object> row in masterGolongan) {
                string[] golonganIdArr = row["golongan_id"].ToString().Split(" ");
                string zoneId = "ANY";
                if(golonganIdArr[0] == "AA" || golonganIdArr[0] == "AE" || golonganIdArr[0] == "JP") {
                    zoneId = golonganIdArr[0];
                }
                golonganIdArr = golonganIdArr.Where(a => a != "AA" && a != "AE" && a != "JP").ToArray();
                string golonganId = String.Join(' ', golonganIdArr);
                int currencyId = 0;
                if(row["golongan_tipe"].ToString() == "DLK") {
                    currencyId = 1; //IDR
                } else if(row["golongan_tipe"].ToString() == "DLN") {
                    currencyId = 2; //USD
                }
                result.Add(
                    new RowData<string, object>() {
                        { "factionid", golonganId },
                        { "zoneid", zoneId },
                        { "currencyid", currencyId },
                        { "perdiemsmoney", row["golongan_perdiems"] },
                        { "pocketmoney", row["golongan_uangsaku"] },
                        { "mealmoney", row["golongan_uangmakan"] },
                        { "laundrymoney", row["golongan_uanglaundry"] },
                    }    
                );
            }

            return result;
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData("master_faction_zone_rate", data);
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterFaction(connections).run();
            new MasterZone(connections).run();
            new MasterCurrency(connections).run();
        }
    }
}
