using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class AspNetUsers : _BaseTask {
        public AspNetUsers(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
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
                    ids = new string[] { "nik" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "relation_user_usergroup",
                    columns = new string[] {
                        "userid",
                        "usergroupid",
                        "is_disabled",
                    },
                    ids = new string[] { "userid", "usergroupid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            return getDataFromMasterEisAktif();
        }

        private MappedData getDataFromMasterEisAktif() {
            MappedData result = new MappedData();

            DataIntegration integration = new DataIntegration(connections);
            string query = @"
                select 
	                uuid_generate_v4() as id,
	                master_eis.nik, 
	                master_eis.""name"", 
	                case when coalesce(TRIM(master_eis.department_code), '') <> '' then 
		                master_eis.department_code
	                else
		                case when coalesce(TRIM(master_eis.division_code), '') <> '' then 
			                master_eis.division_code
		                else
			                master_eis.directorate_code
		                end
	                end as unit_code,
	                master_eis.email, 
	                master_eis.phone
                from 
	                dblink(
		                'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		                '
			                SELECT 
				                ""NIK"" as nik, 
				                ""Nama"" as name,
				                department_code,
				                division_code,
				                directorate_code,
				                email,
				                ""NomorHP"" as phone
			                FROM 
				                hris.""MasterEisAktif""
		                '::text
	                ) master_eis (
		                nik character varying(10), 
		                ""name"" character varying(100), 
		                department_code character varying(50), 
		                division_code character varying(50), 
		                directorate_code character varying(50), 
		                email character varying(200), 
		                phone character varying(25)
	                )
                ;
            ";

            var surplus_conn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
            var rs = QueryUtils.executeQuery(surplus_conn, query);

            foreach(var row in rs) {
                string newDeptId = null;
                string unitCode = Utils.obj2str(row["unit_code"]);
                if(unitCode != null) {
                    newDeptId = integration.getDepartmentFromHrisDeptId(unitCode);
                }

                result.addData(
                    "AspNetUsers",
                    new RowData<ColumnName, object>() {
                        { "id", row["id"]},
                        { "nik", row["nik"]},
                        { "fullname", row["name"]},
                        { "username", row["name"]},
                        { "departmentid", newDeptId},
                        { "isdisabled", false},
                        { "occupationid", 1},
                        { "usergroupid", 1},
                        { "emailconfirmed", false},
                        { "phonenumberconfirmed", false},
                        { "twofactorenabled", false},
                        { "lockoutenabled", false},
                        { "accessfailedcount", 0},
                        { "email", row["email"]},
                        { "phonenumber", row["phone"]}
                    }
                );

                result.addData(
                    "relation_user_usergroup",
                    new RowData<ColumnName, object>() {
                        { "userid", row["nik"]},
                        { "usergroupid", 1},
                        { "is_disabled", false},
                    }
                );
            }

            return result;
        }

        private MappedData getDataFromJSON() {
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
            {
                new _Department(connections).run();
            }
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
