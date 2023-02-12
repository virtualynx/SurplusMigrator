using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterTaxObject : _BaseTask {
        public MasterTaxObject(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_tax_object",
                    columns = new string[] {
                        "taxobjectid",
                        "name",
                        "taxcategoryid",
                        "accountid_clearingpph",
                        "accountid_debtpph",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "taxobjectid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="Tax category", ordinal=1 },
                new ExcelColumn(){ name="Tax Object", ordinal=3 },
                new ExcelColumn(){ name="Keterangan Tax Object", ordinal=4 },
                new ExcelColumn(){ name="Account_Id Clearing PPh", ordinal=6 },
                new ExcelColumn(){ name="Account_Id Hutang PPh", ordinal=8 },
            };

            var excelData = Utils.getDataFromExcel("Analisa Account Relation - 2.xlsx", columns, "Tax Object").ToArray();
            excelData = excelData.Where(a => Utils.obj2str(a["Tax category"]) != "Tax category").ToArray();

            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            foreach(var row in excelData) {
                string taxObjectId = Utils.obj2str(row["Tax Object"]);

                var existing = QueryUtils.executeQuery(surplusConn, "select taxobjectid from master_tax_object where taxobjectid = @taxobjectid", 
                        new Dictionary<string, object> {
                            { "@taxobjectid", taxObjectId }
                        }
                    );

                if(existing.Length > 0) {
                    var affected = QueryUtils.executeQuery(surplusConn,
                        "update master_tax_object set accountid_clearingpph = @accountid_clearingpph, accountid_debtpph = @accountid_debtpph where taxobjectid = @taxobjectid",
                        new Dictionary<string, object> {
                            { "@accountid_clearingpph", Utils.obj2str(row["Account_Id Clearing PPh"]) },
                            { "@accountid_debtpph", Utils.obj2str(row["Account_Id Hutang PPh"]) },
                            { "@taxobjectid", taxObjectId }
                        }
                    );
                }

                result.addData(
                    "master_tax_object",
                    new RowData<ColumnName, object>() {
                        { "taxobjectid", taxObjectId},
                        { "name", Utils.obj2str(row["Keterangan Tax Object"])},
                        { "taxcategoryid", getTaxCategoryId(row["Tax category"])},
                        { "accountid_clearingpph", Utils.obj2str(row["Account_Id Clearing PPh"])},
                        { "accountid_debtpph", Utils.obj2str(row["Account_Id Hutang PPh"])},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterTaxCategory(connections).run();
        }

        private static Dictionary<string, int> taxCategoryMap = null;
        private int getTaxCategoryId(object name) {
            if(taxCategoryMap == null) {
                taxCategoryMap = new Dictionary<string, int>();
                var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
                var categories = QueryUtils.executeQuery(surplusConn, "select * from master_tax_category");

                foreach(var row in categories) {
                    taxCategoryMap[Utils.obj2str(row["name"]).ToLower()] = Utils.obj2int(row["taxcategoryid"]);
                }
            }

            return taxCategoryMap[Utils.obj2str(name).ToLower()];
        }
    }
}
