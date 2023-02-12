using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

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

        public string getJournalReferenceTypeId(string tjournalid) {
            Dictionary<string, string> referenceTypeMap = new Dictionary<string, string>() {
                { "AP", "jurnal_ap" },
                { "CN", null },
                { "DN", null },
                { "JV", "jurnal_jv" },
                { "OC", null },
                { "OR", null },
                { "PV", "payment" },
                { "RV", null },
                { "SA", null },
                { "ST", "payment" },
            };

            return referenceTypeMap[getJournalIdPrefix(tjournalid)];
        }

        public string getJournalIdPrefix(string tjournalid) {
            Match match = Regex.Match(tjournalid, @"[a-zA-Z]+");

            return match.Groups[0].Value;
        }

        public void fillJournalDetailTrackingFields(List<RowData<ColumnName, object>> inputs) {
            List<string> journalIds = new List<string>();

            foreach(RowData<ColumnName, object> row in inputs) {
                string jurnal_id = Utils.obj2str(row["jurnal_id"]);
                if(!journalIds.Contains(jurnal_id)) {
                    journalIds.Add(jurnal_id);
                }
            }

            SqlConnection conn = (SqlConnection)connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault().GetDbConnection();
            SqlCommand command = new SqlCommand("select jurnal_id, created_dt, created_by, jurnal_isdisabled, jurnal_isdisableddt, jurnal_isdisabledby from [dbo].[transaksi_jurnal] where jurnal_id in ('" + String.Join("','", journalIds) + "')", conn);
            SqlDataReader dataReader = command.ExecuteReader();

            Dictionary<string, RowData<ColumnName, object>> queriedJournals = new Dictionary<string, RowData<ColumnName, object>>();
            while(dataReader.Read()) {
                string journalId = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("jurnal_id"))).ToUpper();
                queriedJournals[journalId] = new RowData<ColumnName, object>() {
                    { "created_dt", dataReader.GetValue(dataReader.GetOrdinal("created_dt")) },
                    { "created_by", dataReader.GetValue(dataReader.GetOrdinal("created_by")) },
                    { "jurnal_isdisabled", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisabled")) },
                    { "jurnal_isdisableddt", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisableddt")) },
                    { "jurnal_isdisabledby", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisabledby")) },
                };
            }
            dataReader.Close();
            command.Dispose();

            foreach(RowData<ColumnName, object> row in inputs) {
                RowData<ColumnName, object> journal = queriedJournals[Utils.obj2str(row["jurnal_id"]).ToUpper()];
                row["created_dt"] = journal["created_dt"];
                row["created_by"] = journal["created_by"];
                row["jurnal_isdisabled"] = journal["jurnal_isdisabled"];
                row["jurnal_isdisableddt"] = journal["jurnal_isdisableddt"];
                row["jurnal_isdisabledby"] = journal["jurnal_isdisabledby"];
            }
        }
    }
}
