using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class Relation_Module_UserGroup : _BaseTask {
        public Relation_Module_UserGroup(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "relation_module_usergroup",
                    columns = new string[] {
                        "moduleid",
                        "usergroupid",
                        "read",
                        "create",
                        "update",
                        "delete",
                        "activate",
                        "approve1",
                        "approve2",
                        "approve3",
                        "approve4",
                        "unapprove1",
                        "unapprove2",
                        "unapprove3",
                        "unapprove4",
                        "approvedetail",
                        "unapprovedetail",
                        "posting",
                        "unposting",
                        "printheader",
                        "printdetail",
                        "printbarcode",
                        "type"
                    },
                    ids = new string[] { "moduleid", "usergroupid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            JsonElement json = Utils.getDataFromJson("relation_module_modulegroup");
            var objEnum = json.EnumerateObject();
            objEnum.MoveNext();
            var firstElement = objEnum.Current.Value;
            var length = firstElement.GetArrayLength();
            for(int a = 0; a < length; a++) {
                var ele = firstElement[a];
                string create = ele.GetProperty("create").ToString().ToLower();
                string read = ele.GetProperty("read").ToString().ToLower();
                string update = ele.GetProperty("update").ToString().ToLower();
                string delete = ele.GetProperty("delete").ToString().ToLower();
                string activate = ele.GetProperty("activate").ToString().ToLower();
                string approve = ele.GetProperty("approve").ToString().ToLower();
                result.addData(
                    "relation_module_usergroup",
                    new RowData<ColumnName, object>() {
                        { "moduleid", Utils.obj2int(ele.GetProperty("moduleid"))},
                        { "usergroupid", Utils.obj2int(ele.GetProperty("modulegroupid"))},
                        { "create", create == "true" ? true : false },
                        { "read", read == "true" ? true : false },
                        { "update", update == "true" ? true : false },
                        { "delete", delete == "true" ? true : false },
                        { "activate", activate == "true" ? true : false },
                        { "approve1", approve == "true" ? true : false },
                        { "approve2", true },
                        { "approve3", true },
                        { "approve4", true },
                        { "unapprove1", true },
                        { "unapprove2", true },
                        { "unapprove3", true },
                        { "unapprove4", true },
                        { "approvedetail", true },
                        { "unapprovedetail", true },
                        { "posting", true },
                        { "unposting", true },
                        { "printheader", true },
                        { "printdetail", true },
                        { "printbarcode", true },
                        { "type",  Utils.obj2str(ele.GetProperty("type"))},
                    }
                );
            }

            return result;
        }
    }
}
