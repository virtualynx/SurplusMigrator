using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Nik = System.String;
using ModuleName = System.String;

namespace SurplusMigrator.Tasks {
    class _Privileges : _BaseTask {
        public _Privileges(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_START").FirstOrDefault(),
                    tableName = "master_crewequipmentstudio",
                    columns = new string[] {
                        "studio_id",
                        "studio_name",
                        "studio_location",
                        "studio_remark",
                        "studio_category_id",
                        "studio_createdby",
                        "studio_createddate",
                        "studio_modifiedby",
                        "studio_modifieddate",
                        "studio_isdisabled",
                        "studio_disableby",
                        "studio_disabledate",
                    },
                    ids = new string[] { "studio_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "application",
                    columns = new string[] {
                        "applicationid",
                        "name",
                        "description",
                        "is_disabled",
                    },
                    ids = new string[] { "applicationid" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "module",
                    columns = new string[] {
                        "name",
                        "description",
                        "applicationid",
                        "is_disabled",
                    },
                    ids = new string[] { "name", "applicationid" }
                }
            };
        }

        protected override void runDependencies() {
            new MasterModule(connections).run();
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "application",
                new RowData<ColumnName, object> {
                    { "applicationid", "surplus"},
                    { "name", "Surplus"},
                    { "description", "Surplus Web Applications"},
                    { "is_disabled", false},
                }
            );

            var modules = getModules();
            foreach(var row in modules) {
                result.addData(
                    "module",
                    new RowData<ColumnName, object> {
                        { "name", row["name"]},
                        { "description", row["name"]+". Endpoint: "+row["endpoint"]},
                        { "applicationid", "surplus"},
                        { "is_disabled", false},
                    }
                );
            }



            return result;
        }

        private RowData<ColumnName, object>[] getModules() {
            var result = new List<RowData<ColumnName, object>>();

            JsonElement json = Utils.getDataFromJson("master_module");
            var objEnum = json.EnumerateObject();
            objEnum.MoveNext();
            var firstElement = objEnum.Current.Value;
            var length = firstElement.GetArrayLength();
            for(int a = 0; a < length; a++) {
                var module = firstElement[a];
                string isDisabledStr = module.GetProperty("is_disabled").ToString().ToLower();
                bool isDisabled = isDisabledStr == "true" ? true : false;

                result.Add(
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

            return result.ToArray();
        }

        private Dictionary<Nik, List<ModuleName>> getOldInsosysPrivilege() {
            var result = new Dictionary<Nik, List<ModuleName>>();

            return result;
        }
    }
}
