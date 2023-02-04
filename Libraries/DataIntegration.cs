using Npgsql;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Libraries {
    class DataIntegration {
        private DbConnection_[] connections;
        private Dictionary<string, int> _currencyIdMaps = null;
        private Dictionary<string, string> _strukturUnitMaps = null;
        private Dictionary<string, string> _newDeptIdMap = null;

        public DataIntegration(DbConnection_[] connections) {
            this.connections = connections;
        }

        public int getCurrencyIdFromShortname(string shortname) {
            int result = getCurrencyIdMaps()["UNKWN"];
            if(getCurrencyIdMaps().ContainsKey(shortname)) {
                result = getCurrencyIdMaps()[shortname];
            }

            return result;
        }

        private Dictionary<string, int> getCurrencyIdMaps() {
            if(_currencyIdMaps == null) {
                _currencyIdMaps = new Dictionary<string, int>();

                DbConnection_ connection_ = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
                NpgsqlConnection conn = (NpgsqlConnection)connection_.GetDbConnection();
                NpgsqlCommand command = new NpgsqlCommand("select currencyid, shortname from \"" + connection_.GetDbLoginInfo().schema + "\".\"master_currency\"", conn);
                NpgsqlDataReader dataReader = command.ExecuteReader();

                while(dataReader.Read()) {
                    int currencyid = Utils.obj2int(dataReader.GetValue(dataReader.GetOrdinal("currencyid")));
                    string shortname = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("shortname")));

                    _currencyIdMaps[shortname] = currencyid;
                }
                dataReader.Close();
                command.Dispose();
            }

            return _currencyIdMaps;
        }

        /// <summary>
        /// Get DepartmentId from Insosys's struktur_unit_id
        /// </summary>
        /// <param name="strukturUnitId"></param>
        /// <returns></returns>
        public string getDepartmentFromStrukturUnit(string strukturUnitId) {
            if(_strukturUnitMaps == null) {
                _strukturUnitMaps = new Dictionary<string, string>();
                ExcelColumn[] columns = new ExcelColumn[] {
                    new ExcelColumn(){ name="id", ordinal=0 },
                    new ExcelColumn(){ name="department_baru", ordinal=2 }
                };

                var excelData = Utils.getDataFromExcel("Department2.xlsx", columns, "Department Migrasi").ToArray();

                foreach(var row in excelData) {
                    string strukturid = row["id"].ToString().Trim();
                    if(strukturid == "0") {
                        _strukturUnitMaps[strukturid] = null;
                    } else {
                        _strukturUnitMaps[strukturid] = row["department_baru"].ToString().Trim();
                    }
                }
            }

            if(strukturUnitId == null || strukturUnitId.Trim().Length == 0) return null;

            return _strukturUnitMaps[strukturUnitId.Trim()];
        }

        public string getDepartmentFromHrisDeptId(string hrisDeptId) {
            if(_newDeptIdMap == null) {
                _newDeptIdMap = new Dictionary<string, string>();

                var conn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
                var datas = QueryUtils.executeQuery(conn, "select departmentid, departmentid_hris from relation_department_surplus_hris");
                foreach(var row in datas) {
                    _newDeptIdMap[row["departmentid_hris"].ToString()] = row["departmentid"].ToString();
                }
            }

            return _newDeptIdMap[hrisDeptId];
        }
    }
}
