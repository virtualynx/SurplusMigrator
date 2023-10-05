using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _ApplyPrivileges : _BaseTask, IExcelSourced {
        private DbConnection_ targetConnection;

        private const int DEFAULT_BATCH_SIZE = 500;

        private const string QUERY_EIS_ACTIVE = @"
            do $$
            begin
	            drop table if exists temp_master_eis;
	            create temp table temp_master_eis as
		            select *
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

	            drop table if exists temp_master_eis2;
	            create temp table if not exists temp_master_eis2 as
		            select 
			            nik, 
			            name,
			            email,
			            phone,
			            (
				            case 
				            when coalesce(TRIM(department_code), '') <> '' then 
					            department_code
				            when coalesce(TRIM(division_code), '') <> '' then 
					            division_code
				            else
					            directorate_code
				            end
			            ) as department_code
		            from 
			            temp_master_eis
	            ;

	            drop table if exists temp_master_eis3;
	            create temp table if not exists temp_master_eis3 as
		            select 
			            eis.nik, 
			            eis.name,
			            --eis.email,
			            --eis.phone,
			            rdsh.departmentid
		            from 
			            temp_master_eis2 eis
			            join relation_department_surplus_hris rdsh on rdsh.departmentid_hris = eis.department_code
	            ;
            end$$;

            select * from temp_master_eis3;
        ";

        private string[] ADMIN_GROUP = new string[] {
            "IT"
        };

        private string[] FINANCE_GROUP = new string[] {
            "ACC",
            "FINDIV",
            "FINDEPT",
            "PCM",
            "AR"
        };

        public _ApplyPrivileges(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            Console.WriteLine("\n");
            MyConsole.Information("Apply Privilege (schema " + targetConnection.GetDbLoginInfo().schema + ")");
            Console.WriteLine();
            Console.Write("Continue performing fix (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            var moduleGroups = getDataFromExcel();

            var moduleGroups_finance = moduleGroups.FirstOrDefault(a => a["group"].ToString() == "finance")?["data"];
            var moduleGroups_finance_arr = ((string[])moduleGroups_finance).Skip(1).Select(a => Utils.obj2int(a)).ToArray();
            var moduleGroups_user1 = moduleGroups.FirstOrDefault(a => a["group"].ToString() == "user1")?["data"];
            var moduleGroups_user1_arr = ((string[])moduleGroups_user1).Skip(1).Select(a => Utils.obj2int(a)).ToArray();

            var eisDatas = QueryUtils.executeQuery(
                targetConnection,
                QUERY_EIS_ACTIVE
            );

            Table masterModuleTable = new Table(
                new TableInfo() {
                    connection = targetConnection,
                    tablename = "master_module",
                    columns = new string[] { "moduleid" },
                    ids = new string[] { "moduleid" },
                }
            );

            var moduleGroups_all_arr = masterModuleTable.getAllData().Select(a => Utils.obj2int(a["moduleid"])).ToArray();

            Table relationModuleUsergroupTable = new Table(
                new TableInfo() {
                    connection = targetConnection,
                    tablename = "relation_module_usergroup",
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
                        "printbarcode"
                    },
                    ids = new string[] { "moduleid", "usergroupid" },
                }
            );

            List<RowData<string, object>> relationModuleUsergroupDatas = new List<RowData<string, object>>();
            //admin modulegroup
            foreach(var module in moduleGroups_all_arr) {
                relationModuleUsergroupDatas.Add(new RowData<string, object>() {
                    {"moduleid", module },
                    {"usergroupid", 1 },
                    {"read", true },
                    {"create", true },
                    {"update", true },
                    {"delete", true },
                    {"activate", true },
                    {"approve1", true },
                    {"approve2", true },
                    {"approve3", true },
                    {"approve4", true },
                    {"unapprove1", true },
                    {"unapprove2", true },
                    {"unapprove3", true },
                    {"unapprove4", true },
                    {"approvedetail", true },
                    {"unapprovedetail", true },
                    {"posting", true },
                    {"unposting", true },
                    {"printheader", true },
                    {"printdetail", true },
                    {"printbarcode", true }
                });
            }

            //finance modulegroup
            foreach(var module in moduleGroups_finance_arr) {
                relationModuleUsergroupDatas.Add(new RowData<string, object>() {
                    {"moduleid", module },
                    {"usergroupid", 2 },
                    {"read", true },
                    {"create", true },
                    {"update", true },
                    {"delete", true },
                    {"activate", true },
                    {"approve1", true },
                    {"approve2", true },
                    {"approve3", true },
                    {"approve4", true },
                    {"unapprove1", true },
                    {"unapprove2", true },
                    {"unapprove3", true },
                    {"unapprove4", true },
                    {"approvedetail", true },
                    {"unapprovedetail", true },
                    {"posting", true },
                    {"unposting", true },
                    {"printheader", true },
                    {"printdetail", true },
                    {"printbarcode", true }
                });
            }

            //user1 modulegroup
            foreach(var module in moduleGroups_user1_arr) {
                relationModuleUsergroupDatas.Add(new RowData<string, object>() {
                    {"moduleid", module },
                    {"usergroupid", 3 },
                    {"read", true },
                    {"create", true },
                    {"update", true },
                    {"delete", true },
                    {"activate", true },
                    {"approve1", true },
                    {"approve2", true },
                    {"approve3", true },
                    {"approve4", true },
                    {"unapprove1", true },
                    {"unapprove2", true },
                    {"unapprove3", true },
                    {"unapprove4", true },
                    {"approvedetail", true },
                    {"unapprovedetail", true },
                    {"posting", true },
                    {"unposting", true },
                    {"printheader", true },
                    {"printdetail", true },
                    {"printbarcode", true }
                });
            }

            Table relationUserUsergroupTable = new Table(
                new TableInfo() {
                    connection = targetConnection,
                    tablename = "relation_user_usergroup",
                    columns = new string[] {
                        "userid",
                        "usergroupid",
                        "is_disabled"
                    },
                    ids = new string[] { "userid", "usergroupid" },
                }
            );

            List<RowData<string, object>> relationUserUsergroupDatas = new List<RowData<string, object>>();
            foreach(var loop in eisDatas) {
                string departmentid = Utils.obj2str(loop["departmentid"]);
                int usergroupid = 3;
                if(ADMIN_GROUP.Contains(departmentid)) {
                    usergroupid = 1;
                } else if(FINANCE_GROUP.Contains(departmentid)) {
                    usergroupid = 2;
                }

                relationUserUsergroupDatas.Add(new RowData<string, object>() {
                    {"userid", Utils.obj2str(loop["nik"]) },
                    {"usergroupid", usergroupid },
                    {"is_disabled", false}
                });
            }

            DbTransaction transaction = targetConnection.GetDbConnection().BeginTransaction(); ;

            string backupTimestamp = DateTime.Now.ToString("yyyy_MM_dd_HHmmss");
            try {
                QueryUtils.executeQuery(
                    targetConnection,
                    "create table relation_module_usergroup_<backupTimestamp> as select * from relation_module_usergroup;"
                    .Replace("<backupTimestamp>", backupTimestamp),
                    null,
                    transaction
                );
                QueryUtils.executeQuery(
                    targetConnection,
                    "TRUNCATE TABLE relation_module_usergroup CONTINUE IDENTITY RESTRICT;",
                    null,
                    transaction
                );
                relationModuleUsergroupTable.insertData(relationModuleUsergroupDatas, transaction);

                QueryUtils.executeQuery(
                    targetConnection,
                    "create table relation_user_usergroup_<backupTimestamp> as select * from relation_user_usergroup;"
                    .Replace("<backupTimestamp>", backupTimestamp),
                    null,
                    transaction
                );
                QueryUtils.executeQuery(
                    targetConnection,
                    "TRUNCATE TABLE relation_user_usergroup CONTINUE IDENTITY RESTRICT;",
                    null,
                    transaction
                );

                relationUserUsergroupTable.insertData(relationUserUsergroupDatas, transaction);

                transaction.Commit();
            }catch(Exception ex) {
                transaction.Rollback();
                throw;
            }
        }

        public RowData<string, object>[] getDataFromExcel() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="moduleid", ordinal=0 },
            };

            List<RowData<string, object>> result = new List<RowData<string, object>>();

            //finance
            List<RowData<string, object>> fin_data = Utils.getDataFromExcel(getExcelFilename(), columns, "finance");
            result.Add(new RowData<string, object>() {
                { "group", "finance" },
                { "data", fin_data.Select(a => Utils.obj2str(a["moduleid"])).ToArray() } 
            });
            

            //user_1
            List<RowData<string, object>> user1_data = Utils.getDataFromExcel(getExcelFilename(), columns, "user1");
            result.Add(new RowData<string, object>() {
                { "group", "user1" },
                { "data", user1_data.Select(a => Utils.obj2str(a["moduleid"])).ToArray() } 
            });


            return result.ToArray();
        }

        public string getExcelFilename() {
            return getOptions("filename");
        }
    }
}
