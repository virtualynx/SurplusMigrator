using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class _Department : _BaseTask {
        Dictionary<string, string> _newDeptIdMap = null;
        Dictionary<string, List<string>> _departmentMaps = new Dictionary<string, List<string>>();
        private string source = "json";

        public _Department(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "hris").FirstOrDefault(),
                    tableName = "Organization_structure",
                    columns = new string[] {
                        "structure_code",
                        "structure_name",
                        "structure_typeid",
                        "structure_scope"
                    },
                    ids = new string[] { "structure_code" }
                },
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_department",
                    columns = new string[] {
                        "departmentid",
                        "name",
                        "shortname",
                        "created_by",
                        "created_date",
                        "is_disabled",
                    },
                    ids = new string[] { "departmentid" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "relation_department_surplus_hris",
                    columns = new string[] {
                        "departmentid",
                        "departmentid_hris",
                        "created_by",
                        "created_date",
                        "is_disabled",
                    },
                    ids = new string[] { "departmentid", "departmentid_hris" }
                }
            };

            if(getOptions("source") != null) {
                source = getOptions("source");
            }
        }

        protected override MappedData getStaticData() {
            if(source == "json") {
                return getDataFromJson();
            } else if(source == "excel") {
                return getDataFromExcel();
            } else {
                throw new Exception("Invalid source option value: "+source+" (valid value are json, excel)");
            }
        }

        #region get from json
        private MappedData getDataFromJson() {
            MappedData result = new MappedData();

            JsonElement json = Utils.getDataFromJson("master_department");
            var objEnum = json.EnumerateObject();
            objEnum.MoveNext();
            var firstElement = objEnum.Current.Value;
            var length = firstElement.GetArrayLength();
            for(int a = 0; a < length; a++) {
                var ele = firstElement[a];

                string isDisabledStr = ele.GetProperty("is_disabled").ToString().ToLower();
                bool isDisabled = isDisabledStr == "true" ? true : false;

                result.addData(
                    "master_department",
                    new RowData<ColumnName, object> {
                        { "departmentid", ele.GetProperty("departmentid").ToString()},
                        { "name", ele.GetProperty("name").ToString()},
                        { "shortname", ele.GetProperty("shortname").ToString()},
                        { "created_by", Utils.obj2str(ele.GetProperty("created_by"))},
                        { "created_date", Utils.stringUtc2datetime(Utils.obj2str(ele.GetProperty("created_date")))},
                        { "is_disabled", isDisabled},
                    }
                );
            }

            json = Utils.getDataFromJson("relation_department_surplus_hris");
            objEnum = json.EnumerateObject();
            objEnum.MoveNext();
            firstElement = objEnum.Current.Value;
            length = firstElement.GetArrayLength();
            for(int a = 0; a < length; a++) {
                var ele = firstElement[a];

                string isDisabledStr = ele.GetProperty("is_disabled").ToString().ToLower();
                bool isDisabled = isDisabledStr == "true" ? true : false;

                result.addData(
                    "relation_department_surplus_hris",
                    new RowData<ColumnName, object> {
                        { "departmentid", ele.GetProperty("departmentid").ToString()},
                        { "departmentid_hris", ele.GetProperty("departmentid_hris").ToString()},
                        { "created_by", Utils.obj2str(ele.GetProperty("created_by"))},
                        { "created_date", Utils.stringUtc2datetime(Utils.obj2str(ele.GetProperty("created_date")))},
                        { "is_disabled", isDisabled},
                    }
                );
            }

            return result;
        }
        #endregion

        #region get from excel
        private MappedData getDataFromExcel() {
            MappedData result = new MappedData();

            var newMappings = getNewMapping();
            newMappings = newMappings.Where(a => Utils.obj2str(a["structure_code"]) != "structure_code").ToArray();

            var newDepartments = (
                from nm in newMappings
                where Utils.obj2str(nm["department_mapped"]) != "department_mapped"
                group nm by Utils.obj2str(nm["department_mapped"]) into nmg
                select nmg.Key
            ).ToArray();

            foreach(var dept in newDepartments) {
                result.addData(
                    "master_department",
                    new RowData<ColumnName, object> {
                        { "departmentid", getNewDeptId(dept)},
                        { "name", dept},
                        { "shortname", dept},
                        { "created_by", DefaultValues.CREATED_BY},
                        { "created_date", DateTime.Now},
                        { "is_disabled", false},
                    }
                );
            }

            //var oldStrukturUnits = getOldInsosysStrukturUnit();
            //foreach(var unit in oldStrukturUnits) {
            //    string strukturunit_id = Utils.obj2str(unit["strukturunit_id"]);
            //    string strukturunit_name = Utils.obj2str(unit["strukturunit_name"]);
            //    result.addData(
            //        "master_department",
            //        new RowData<ColumnName, object> {
            //            { "departmentid", strukturunit_id},
            //            { "name", strukturunit_name},
            //            { "shortname", strukturunit_name},
            //            { "created_by", DefaultValues.CREATED_BY},
            //            { "created_date", DateTime.Now},
            //            { "is_disabled", false},
            //        }
            //    );
            //}

            foreach(var row in newMappings) {
                string newDeptCode = getNewDeptId(Utils.obj2str(row["department_mapped"]));
                string structureCode = Utils.obj2str(row["structure_code"]);
                if(!_departmentMaps.ContainsKey(newDeptCode)) {
                    _departmentMaps[newDeptCode] = new List<string>();
                }
                if(!_departmentMaps[newDeptCode].Contains(structureCode)) {
                    _departmentMaps[newDeptCode].Add(structureCode);
                }
            }

            foreach(var row in _departmentMaps) {
                string newDeptCode = row.Key;
                List<string> hrisDepts = row.Value;

                foreach(string hrisDept in hrisDepts) {
                    result.addData(
                        "relation_department_surplus_hris",
                        new RowData<ColumnName, object> {
                            { "departmentid", newDeptCode},
                            { "departmentid_hris", hrisDept},
                            { "created_by", DefaultValues.CREATED_BY},
                            { "created_date", DateTime.Now},
                            { "is_disabled", false},
                        }
                    );
                }
            }

            return result;
        }

        private string getNewDeptId(string newDeptName) {
            if(_newDeptIdMap == null) {
                _newDeptIdMap = new Dictionary<string, string>();

                JsonElement json = Utils.getDataFromJson("new_department_id_mapping");
                var objEnum = json.EnumerateObject();
                objEnum.MoveNext();
                var firstElement = objEnum.Current.Value;
                var length = firstElement.GetArrayLength();
                for(int a = 0; a < length; a++) {
                    var ele = firstElement[a];
                    string id = ele.GetProperty("id").ToString().ToUpper();
                    string name = ele.GetProperty("name").ToString().ToUpper();

                    _newDeptIdMap[name] = id;
                }
            }

            return _newDeptIdMap[newDeptName];
        }

        private RowData<ColumnName, object>[] getNewMapping() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="structure_code", ordinal=0 },
                new ExcelColumn(){ name="structure_name", ordinal=1 },
                new ExcelColumn(){ name="unit", ordinal=2 },
                new ExcelColumn(){ name="structure_typeid", ordinal=3 },
                new ExcelColumn(){ name="structure_scope", ordinal=4 },
                new ExcelColumn(){ name="department_mapped", ordinal=5 }
            };

            return Utils.getDataFromExcel("Department.xlsx", columns).ToArray();
        }

        private RowData<ColumnName, object>[] getOldInsosysStrukturUnit() {
            var conn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").First();
            var res = QueryUtils.executeQuery(conn, "select strukturunit_id, strukturunit_name from master_strukturunit");

            return res;
        }
        #endregion
    }
}
