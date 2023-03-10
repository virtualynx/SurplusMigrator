using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using AccountId = System.String;

namespace SurplusMigrator.Tasks {
    class MasterAccountCategoryDetail : _BaseTask {
        private Dictionary<string, int> accCategoryMap = null;
        public MasterAccountCategoryDetail(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_account_category_detail",
                    columns = new string[] {
                        //"accountcategorydetailid",
                        "accountcategoryid",
                        "accountid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "accountcategoryid", "accountid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            var apStatements = getStatementAp();
            foreach(var acc_id in apStatements) {
                int accCategoryId = getAccountCategory("AP|ACCOUNT STATEMENT AP");
                result.addData(
                    "master_account_category_detail",
                    new RowData<ColumnName, object>() {
                        { "accountcategoryid", accCategoryId},
                        { "accountid", acc_id},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }

            var umStatements = getStatementUM();
            foreach(var acc_id in umStatements) {
                int accCategoryId = getAccountCategory("UM|ACCOUNT STATEMENT UM");
                result.addData(
                    "master_account_category_detail",
                    new RowData<ColumnName, object>() {
                        { "accountcategoryid", accCategoryId},
                        { "accountid", acc_id},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }

            var accAgingAP = getAgingAp();
            foreach(var acc_id in accAgingAP) {
                int accCategoryId = getAccountCategory("AP|AGING AP");
                result.addData(
                    "master_account_category_detail",
                    new RowData<ColumnName, object>() {
                        { "accountcategoryid", accCategoryId},
                        { "accountid", acc_id},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }

            var accAgingAR = getAgingAr();
            foreach(var acc_id in accAgingAR) {
                int accCategoryId = getAccountCategory("AR|AGING AR");
                result.addData(
                    "master_account_category_detail",
                    new RowData<ColumnName, object>() {
                        { "accountcategoryid", accCategoryId},
                        { "accountid", acc_id},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }

            var arStatements = getStatementAR();
            foreach(var acc_id in arStatements) {
                int accCategoryId = getAccountCategory("AR|ACCOUNT STATEMENT AR");
                result.addData(
                    "master_account_category_detail",
                    new RowData<ColumnName, object>() {
                        { "accountcategoryid", accCategoryId},
                        { "accountid", acc_id},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }

            return result;
        }

        private int getAccountCategory(string tag) {
            if(accCategoryMap == null) {
                accCategoryMap = new Dictionary<string, int>();
                var conn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
                var rs = QueryUtils.executeQuery(conn, "select * from master_account_category");
                foreach(var row in rs) {
                    string saveTag = row["type"].ToString() + "|" + row["name"].ToString();
                    accCategoryMap.Add(saveTag, Utils.obj2int(row["accountcategoryid"]));
                }
            }

            return accCategoryMap[tag];
        }

        private AccountId[] getStatementAp() {
            var result = new List<AccountId>();

            var conn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();
            var rs = QueryUtils.executeQuery(conn, "select acc_id from master_account_ap_statement");
            foreach(var row in rs) {
                result.Add(row["acc_id"].ToString());
            }

            return result.ToArray();
        }

        private AccountId[] getStatementUM() {
            var result = new List<AccountId>();

            var conn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();
            var rs = QueryUtils.executeQuery(conn, "select acc_id from master_account_um_statement");
            foreach(var row in rs) {
                result.Add(row["acc_id"].ToString());
            }

            return result.ToArray();
        }

        private AccountId[] getAgingAp() {
            var result = new List<AccountId>();

            var conn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();
            var rs = QueryUtils.executeQuery(conn, "select acc_id_start, acc_id_end from master_accaging where LOWER(accaging_id) = 'ap'");
            var inBetween = new List<string>();
            foreach(var row in rs) {
                string start = Utils.obj2str(row["acc_id_start"]);
                string end = Utils.obj2str(row["acc_id_end"]);
                if(start == end) {
                    result.Add(start);
                } else {
                    inBetween.Add(start+"-"+end);
                }
            }

            var conn2 = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
            foreach(var row in inBetween) {
                string[] between = row.Split("-");
                var rs2 = QueryUtils.executeQuery(conn2, "select accountid from master_account where '" + between[0] +"' <= accountid and accountid <= '" + between[1] +"'");
                foreach(var row_rs in rs2) {
                    result.Add(row_rs["accountid"].ToString());
                }
            }

            return result.ToArray();
        }

        private AccountId[] getAgingAr() {
            var result = new List<AccountId>();

            var conn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();
            var rs = QueryUtils.executeQuery(conn, "select acc_id_start, acc_id_end from master_accaging where LOWER(accaging_id) = 'ar'");
            var inBetween = new List<string>();
            foreach(var row in rs) {
                string start = Utils.obj2str(row["acc_id_start"]);
                string end = Utils.obj2str(row["acc_id_end"]);
                if(start == end) {
                    result.Add(start);
                } else {
                    inBetween.Add(start + "-" + end);
                }
            }

            var conn2 = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
            foreach(var row in inBetween) {
                string[] between = row.Split("-");
                var rs2 = QueryUtils.executeQuery(conn2, "select accountid from master_account where '" + between[0] + "' <= accountid and accountid <= '" + between[1] + "'");
                foreach(var row_rs in rs2) {
                    result.Add(row_rs["accountid"].ToString());
                }
            }

            return result.ToArray();
        }

        private AccountId[] getStatementAR() {
            var result = new List<AccountId>();

            var conn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
            string query = @"
                select accountid from master_account
                where
                    accountid in ([in_params])
                    or accountid like '215%'
            ";
            string[] accList = new string[] {
                "1051000",
                "1051040",
                "1051050",
                "1052010",
                "1052060",
                "1070020",
                "1070010",
                "1070022",
                "1070050",
                "1075000",
                "2031030",
                "2031040",
                "2031050",
                "2200020"
            };
            query = query.Replace("[in_params]", "'" + String.Join("','", accList) + "'");
            var rs = QueryUtils.executeQuery(conn, query);
            foreach(var row in rs) {
                result.Add(row["accountid"].ToString());
            }

            return result.ToArray();
        }

        protected override void runDependencies() {
            new MasterAccount(connections).run();
            new MasterAccountCategory(connections).run();
        }
    }
}
