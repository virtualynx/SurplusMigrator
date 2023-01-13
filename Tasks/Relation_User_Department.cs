using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class Relation_User_Department : _BaseTask {
        public Relation_User_Department(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "relation_user_department",
                    columns = new string[] {
                        "nik",
                        "departmentid",
                        "created_date",
                        "created_by",
                        "is_disabled",
                    },
                    ids = new string[] { "nik", "departmentid" }
                }
            };
        }

        protected override void runDependencies() {
            new AspNetUsers(connections).run();
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            string query = @"
                select 
	                master_eis.nik, 
	                case when coalesce(TRIM(master_eis.department_code), '') <> '' then 
		                master_eis.department_code
	                else
		                case when coalesce(TRIM(master_eis.division_code), '') <> '' then 
			                master_eis.division_code
		                else
			                master_eis.directorate_code
		                end
	                end as unit_code
                from 
	                dblink(
		                'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		                '
			                SELECT 
				                ""NIK"" as nik, 
				                ""Nama"" as name,
				                department_code,
				                division_code,
				                directorate_code
			                FROM 
				                hris.""MasterEisAktif""
		                '::text
	                ) master_eis (
		                nik character varying(10), 
		                ""name"" character varying(100), 
		                department_code character varying(50), 
		                division_code character varying(50), 
		                directorate_code character varying(50)
	                )
                    join ""<schema>"".""AspNetUsers"" as ""user"" on ""user"".nik = master_eis.nik
                ;
            ";

            var surplus_conn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
            query = query.Replace("<schema>", surplus_conn.GetDbLoginInfo().schema);

            var rs = QueryUtils.executeQuery(surplus_conn, query);

            rs = rs.Where(a => a["unit_code"]!=null).ToArray();

            foreach(var row in rs) {
                result.addData(
                    "relation_user_department",
                    new RowData<ColumnName, object>() {
                        { "nik", row["nik"]},
                        { "departmentid", row["unit_code"]},
                        { "created_date", DateTime.Now},
                        { "created_by", DefaultValues.CREATED_BY},
                        { "is_disabled", false},
                    }
                );
            }

            return result;
        }
    }
}
