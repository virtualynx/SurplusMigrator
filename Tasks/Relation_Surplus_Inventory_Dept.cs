using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class Relation_Surplus_Inventory_Dept : _BaseTask {
        public Relation_Surplus_Inventory_Dept(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "relation_hris_inventory_department",
                    columns = new string[] {
                        "hris_id",
                        "inventory_id",
                        "inventory_name",
                    },
                    ids = new string[] { "hris_id", "inventory_id" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            JsonElement json = Utils.getDataFromJson("relation_hris_inventory_department");
            var objEnum = json.EnumerateObject();
            objEnum.MoveNext();
            var firstElement = objEnum.Current.Value;
            var length = firstElement.GetArrayLength();
            for(int a = 0; a < length; a++) {
                var ele = firstElement[a];
                string hris_id = ele.GetProperty("hris_id").ToString();
                string inventory_id = ele.GetProperty("inventory_id").ToString();
                string inventory_name = ele.GetProperty("inventory_name").ToString();

                result.addData(
                    "relation_hris_inventory_department",
                    new RowData<ColumnName, object>() {
                        { "hris_id", hris_id},
                        { "inventory_id", inventory_id},
                        { "inventory_name", inventory_name},
                    }
                );
            }

            return result;
        }
    }
}
