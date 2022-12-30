using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class AspNetUsers : _BaseTask {
        public AspNetUsers(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "AspNetUsers",
                    columns = new string[] {
                        "id",
                        "nik",
                        "fullname",
                        "username",
                        "departmentid",
                        "is_disabled",
                        "occupationid",
                        "usergroupid",
                        "emailconfirmed",
                        "phonenumberconfirmed",
                        "twofactorenabled",
                        "lockoutenabled",
                        "accessfailedcount",
                        "email",
                        "phonenumber"
                    },
                    ids = new string[] { "id" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            JsonElement json = Utils.getDataFromJson("_AspNetUsers_");
            var objEnum = json.EnumerateObject();
            objEnum.MoveNext();
            var firstElement = objEnum.Current.Value;
            var length = firstElement.GetArrayLength();
            for(int a = 0; a < length; a++) {
                var ele = firstElement[a];
                string isdisabled = ele.GetProperty("isdisabled").ToString().ToLower();
                string emailconfirmed = ele.GetProperty("emailconfirmed").ToString().ToLower();
                string phonenumberconfirmed = ele.GetProperty("phonenumberconfirmed").ToString().ToLower();
                string twofactorenabled = ele.GetProperty("twofactorenabled").ToString().ToLower();
                string lockoutenabled = ele.GetProperty("lockoutenabled").ToString().ToLower();

                result.addData(
                    "AspNetUsers",
                    new RowData<ColumnName, object>() {
                        { "id", Utils.obj2str(ele.GetProperty("id"))},
                        { "nik", Utils.obj2str(ele.GetProperty("nik"))},
                        { "fullname", Utils.obj2str(ele.GetProperty("fullname"))},
                        { "username", Utils.obj2str(ele.GetProperty("username"))},
                        { "departmentid", Utils.obj2str(ele.GetProperty("departmentid"))},
                        { "isdisabled", isdisabled == "true" ? true : false},
                        { "occupationid", Utils.obj2int(ele.GetProperty("occupationid"))},
                        { "usergroupid", Utils.obj2int(ele.GetProperty("modulegroupid"))},
                        { "emailconfirmed", emailconfirmed == "true" ? true : false},
                        { "phonenumberconfirmed", phonenumberconfirmed == "true" ? true : false},
                        { "twofactorenabled", twofactorenabled == "true" ? true : false},
                        { "lockoutenabled", lockoutenabled == "true" ? true : false},
                        { "accessfailedcount", Utils.obj2int(ele.GetProperty("accessfailedcount"))},
                        { "email", Utils.obj2str(ele.GetProperty("email"))},
                        { "phonenumber", Utils.obj2str(ele.GetProperty("phonenumber"))}
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterOccupation(connections).run();
            {
                {
                    new MasterModule(connections).run();
                }
                new MasterUserGroup(connections).run();
            }
            new Relation_Module_UserGroup(connections).run();
        }
    }
}
