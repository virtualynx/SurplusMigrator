using SurplusMigrator.Models;
using SurplusMigrator.Models.Others;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data;
using System.IO;
using System.Text.Json;
using Excel = Microsoft.Office.Interop.Excel;
using System.Runtime.InteropServices;

namespace SurplusMigrator.Libraries {
    class Utils {
        public static int obj2int(object o) {
            if(o == null) return 0;
            return Int32.Parse(o.ToString());
        }
        public static long obj2long(object o) {
            if(o == null) return 0;
            return Int64.Parse(o.ToString());
        }
        public static decimal obj2decimal(object o) {
            if(o == null) return 0;
            return Decimal.Parse(o.ToString());
        }
        public static bool obj2bool(object o) {
            if(o == null) return false;
            return Utils.obj2int(o) == 0 ? false : true;
        }
        public static string obj2str(object o) {
            if(o == null) return null;
            string result = o.ToString().Trim();
            if(result.Length == 0) return null;
            return result;
        }
        public static DateTime obj2datetime(object o) {
            if(o == null) throw new Exception("obj2datetime argument is null");
            return Convert.ToDateTime(o);
        }
        public static DateTime? obj2datetimeNullable(object o) {
            if(o == null) return null;
            return Convert.ToDateTime(o);
        }
        public static string getElapsedTimeString(long milliseconds, bool showMilliseconds = false) {
            string format = @"hh\:mm\:ss";
            if(showMilliseconds) {
                format = @"hh\:mm\:ss\.fff";
            }
            return TimeSpan.FromMilliseconds(milliseconds).ToString(format);
        }
        public static T loadJson<T>(string filename) {
            string path = System.Environment.CurrentDirectory + "\\" + filename;
            if(File.Exists(path)) {
                using(StreamReader r = new StreamReader(path)) {
                    string jsonText = r.ReadToEnd();
                    return JsonSerializer.Deserialize<T>(jsonText);
                }
            } else {
                throw new FileNotFoundException("File "+path+" not found");
            }
        }
        public static void saveJson(string filename, dynamic jsonObject) {
            string path = System.Environment.CurrentDirectory + "\\" + filename;
            File.WriteAllText(path, JsonSerializer.Serialize(jsonObject));
        }
        public static void logMissingReference(Type taskType, DateTime startedAt, MissingReference missingReference, List<dynamic> missingRefIds) {
            string filename = "log_(" + taskType.Name + ")_nullified_missing_reference_to_(" + missingReference.referencedTableName + ")_" + startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
            string savePath = System.Environment.CurrentDirectory + "\\" + filename;

            if(File.Exists(savePath)) {
                using(StreamReader r = new StreamReader(savePath)) {
                    string jsonText = r.ReadToEnd();
                    missingReference = JsonSerializer.Deserialize<MissingReference>(jsonText);
                }
            }

            missingReference.referencedIds.AddRange(missingRefIds);
            File.WriteAllText(savePath, JsonSerializer.Serialize(missingReference));
        }

        public static List<RowData<ColumnName, object>> getFromExcel(string filename, ExcelColumn[] columns, string sheetName = null) {
            List<RowData<ColumnName, object>> result = new List<RowData<ColumnName, object>>();

            string sConnection = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=<filename>;Extended Properties=\"Excel 12.0;HDR=No;IMEX=1\"";
            sConnection = sConnection.Replace("<filename>", GlobalConfig.getExcelSourcesPath() + "\\" + filename);

            OleDbConnection oleExcelConnection = new OleDbConnection(sConnection);
            oleExcelConnection.Open();

            DataTable dtTablesList = oleExcelConnection.GetSchema("Tables");

            if(sheetName == null && dtTablesList.Rows.Count > 0) {
                sheetName = dtTablesList.Rows[0]["TABLE_NAME"].ToString();
            }
            dtTablesList.Clear();
            dtTablesList.Dispose();

            if(sheetName != null) {
                OleDbCommand oleExcelCommand = oleExcelConnection.CreateCommand();
                oleExcelCommand.CommandText = "Select * From [" + sheetName + "$]";
                oleExcelCommand.CommandType = CommandType.Text;

                OleDbDataReader oleExcelReader = oleExcelCommand.ExecuteReader();

                int nOutputRow = 0;
                while(oleExcelReader.Read()) {
                    RowData<ColumnName, object> rowData = new RowData<ColumnName, object>();
                    foreach(ExcelColumn column in columns) {
                        rowData.Add(column.name, oleExcelReader.GetValue(column.ordinal));
                    }
                    result.Add(rowData);
                    nOutputRow++;
                }
                oleExcelReader.Close();
            }

            oleExcelConnection.Close();

            return result;
        }

        public static List<RowData<ColumnName, object>> getFromExcel_v2(string filename, ExcelColumn[] columns, string sheetName = null) {
            List<RowData<ColumnName, object>> result = new List<RowData<ColumnName, object>>();

            Excel.Application xlApp = new Excel.Application();
            string exeDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            Excel.Workbook xlWorkbook = xlApp.Workbooks.Open(Path.Combine(exeDir, GlobalConfig.getExcelSourcesPath() + "\\" + filename));
            Excel._Worksheet xlWorksheet = null;
            if(sheetName == null) {
                xlWorksheet = (Excel._Worksheet)xlWorkbook.Sheets[1];
            } else {
                foreach(Excel._Worksheet sheet in xlWorkbook.Sheets) {
                    if(sheet.Name == sheetName) {
                        xlWorksheet = sheet;
                    }
                }
            }
            if(sheetName != null && xlWorksheet == null) {
                throw new Exception("Sheet name [" + sheetName + "] cannot be found");
            }
            Excel.Range xlRange = xlWorksheet.UsedRange;

            int rowCount = xlRange.Rows.Count;
            int colCount = xlRange.Columns.Count;

            //iterate over the rows and columns and print to the console as it appears in the file
            //excel is not zero based!!
            for(int i = 1; i <= rowCount; i++) {
                for(int j = 1; j <= colCount; j++) {
                    //new line
                    if(j == 1) {
                        Console.Write("\r\n");
                    }

                    //write the value to the console
                    if(xlRange.Cells[i, j] != null && xlRange.Cells[i, j] != null) {
                        var test = xlRange.Cells[i, j];
                        Console.Write(xlRange.Cells[i, j].ToString() + "\t");
                    }
                }
            }

            //cleanup
            GC.Collect();
            GC.WaitForPendingFinalizers();

            //rule of thumb for releasing com objects:
            //  never use two dots, all COM objects must be referenced and released individually
            //  ex: [somthing].[something].[something] is bad

            //release com objects to fully kill excel process from running in the background
            Marshal.ReleaseComObject(xlRange);
            Marshal.ReleaseComObject(xlWorksheet);

            //close and release
            xlWorkbook.Close();
            Marshal.ReleaseComObject(xlWorkbook);

            //quit and release
            xlApp.Quit();
            Marshal.ReleaseComObject(xlApp);

            return result;
        }
    }
}
