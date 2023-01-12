using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class MasterSetting : _BaseTask {
        public MasterSetting(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_setting",
                    columns = new string[] {
                        "settingid",
                        "name",
                        "value",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "name" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            JsonElement json = Utils.getDataFromJson("master_setting");
            var objEnum = json.EnumerateObject();
            objEnum.MoveNext();
            var firstElement = objEnum.Current.Value;
            var length = firstElement.GetArrayLength();
            for(int a = 0; a < length; a++) {
                var module = firstElement[a];
                string isDisabledStr = module.GetProperty("is_disabled").ToString().ToLower();
                bool isDisabled = isDisabledStr == "true" ? true : false;

                result.addData(
                    "master_setting",
                    new RowData<ColumnName, object>() {
                        { "settingid", Utils.obj2int(module.GetProperty("settingid"))},
                        { "name", Utils.obj2str(module.GetProperty("name"))},
                        { "value", Utils.obj2str(module.GetProperty("value"))},
                        { "created_date", Utils.stringUtc2datetime(Utils.obj2str(module.GetProperty("created_date")))},
                        { "created_by", getAuthInfo(Utils.obj2str(module.GetProperty("created_by")), true)},
                        { "is_disabled", isDisabled}
                    }
                );
            }

            return result;
        }
    }
}
