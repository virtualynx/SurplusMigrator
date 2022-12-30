using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class MasterModule : _BaseTask {
        public MasterModule(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_module",
                    columns = new string[] {
                        "moduleid",
                        "name",
                        "endpoint",
                        "type",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "moduleid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            JsonElement json = Utils.getDataFromJson("master_module");
            var objEnum = json.EnumerateObject();
            objEnum.MoveNext();
            var firstElement = objEnum.Current.Value;
            var length = firstElement.GetArrayLength();
            for(int a = 0; a < length; a++) {
                var module = firstElement[a];
                string isDisabledStr = module.GetProperty("is_disabled").ToString().ToLower();
                bool isDisabled = isDisabledStr == "true" ? true : false;
                result.addData(
                    "master_module",
                    new RowData<ColumnName, object>() {
                        { "moduleid", Utils.obj2int(module.GetProperty("moduleid"))},
                        { "name", Utils.obj2str(module.GetProperty("name"))},
                        { "endpoint", Utils.obj2str(module.GetProperty("endpoint"))},
                        { "type", Utils.obj2str(module.GetProperty("type"))},
                        { "created_date", Utils.stringUtc2datetime(Utils.obj2str(module.GetProperty("created_date")))},
                        { "created_by", Utils.obj2str(module.GetProperty("created_by"))},
                        { "is_disabled", isDisabled}
                    }
                );
            }

            return result;
        }
    }
}
