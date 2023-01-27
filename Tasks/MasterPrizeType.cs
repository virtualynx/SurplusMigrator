using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterPrizeType : _BaseTask {
        public MasterPrizeType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_prize_type",
                    columns = new string[] {
                        "prizetypeid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "prizetypeid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_prize_type",
                new RowData<ColumnName, object>() {
                    { "prizetypeid", 1},
                    { "name", "Alternatif"},
                    { "created_date", DateTime.Now},
                    { "created_by", DefaultValues.CREATED_BY },
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_prize_type",
                new RowData<ColumnName, object>() {
                    { "prizetypeid", 2},
                    { "name", "Barang"},
                    { "created_date", DateTime.Now},
                    { "created_by", DefaultValues.CREATED_BY },
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_prize_type",
                new RowData<ColumnName, object>() {
                    { "prizetypeid", 3},
                    { "name", "Uang"},
                    { "created_date", DateTime.Now},
                    { "created_by", DefaultValues.CREATED_BY },
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
