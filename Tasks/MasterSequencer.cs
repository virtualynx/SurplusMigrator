using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class MasterSequencer : _BaseTask {
        public MasterSequencer(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_sequencer",
                    columns = new string[] {
                        "sequencerid",
                        "type",
                        "lastid",
                        "lastmonth"
                    },
                    ids = new string[] { "sequencerid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            JsonElement json = Utils.getDataFromJson("master_sequencer");
            var objEnum = json.EnumerateObject();
            objEnum.MoveNext();
            var firstElement = objEnum.Current.Value;
            var length = firstElement.GetArrayLength();
            for(int a = 0; a < length; a++) {
                var ele = firstElement[a];
                result.addData(
                    "master_sequencer",
                    new RowData<ColumnName, object>() {
                        { "sequencerid", Utils.obj2int(ele.GetProperty("sequencerid"))},
                        { "type", Utils.obj2str(ele.GetProperty("type"))},
                        { "lastid", Utils.obj2int(ele.GetProperty("lastid"))},
                        { "lastmonth", Utils.stringUtc2datetime(Utils.obj2str(ele.GetProperty("lastmonth")))}
                    }
                );
            }

            return result;
        }
    }
}
