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
using System.Globalization;
using System.Linq;
using System.Collections;
using System.Security.Policy;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using System.Diagnostics;

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
            Type type = o.GetType();
            if(type == typeof(int) || type == typeof(Byte)) {
                return Utils.obj2int(o) == 0 ? false : true;
            } else if(type == typeof(Decimal)) {
                return Utils.obj2decimal(o) == 0 ? false : true;
            } else if(type == typeof(bool)) {
                return (bool)o;
            } if(type == typeof(string)) {
                string boolStr = Utils.obj2str(o).ToLower();
                return Boolean.Parse(boolStr);
            } else {
                throw new Exception("Unable to convert \"" + o.ToString() + "\" (" + type.ToString() +") into boolean");
            }
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
        public static DateTime? stringUtc2datetime(string str) {
            if(str == null) return null;
            return DateTime.ParseExact(str, "yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);
        }
        public static DateTime? obj2datetimeNullable(object o) {
            if(o == null) return null;
            return Convert.ToDateTime(o);
        }
        public static object[] obj2array(object o) {
            if(o.GetType().IsArray) {
                return (object[])o;
            } else if(isList(o)) {
                return new List<Object>((IEnumerable<Object>)o).ToArray();
            } else {
                throw new Exception("Argument type is neither an Array or List, type: "+o.GetType().FullName);
            }
        }
        public static string getElapsedTimeString(long milliseconds, bool showMilliseconds = false) {
            string format = @"hh\:mm\:ss";
            if(showMilliseconds) {
                format = @"hh\:mm\:ss\.fff";
            }
            return TimeSpan.FromMilliseconds(milliseconds).ToString(format);
        }
        public static bool isList(object o) {
            if(o == null) return false;
            return o is IList &&
                   o.GetType().IsGenericType &&
                   o.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>));
        }
        public static bool isDictionary(object o) {
            if(o == null) return false;
            return o is IDictionary &&
                   o.GetType().IsGenericType &&
                   o.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(Dictionary<,>));
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

        public static List<RowData<ColumnName, object>> getDataFromExcel(string filename, ExcelColumn[] columns, string sheetName = null) {
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
                if(!sheetName.EndsWith("$")) {
                    sheetName = sheetName + "$";
                }

                OleDbCommand oleExcelCommand = oleExcelConnection.CreateCommand();
                oleExcelCommand.CommandText = "Select * From [" + sheetName + "]";
                oleExcelCommand.CommandType = CommandType.Text;

                OleDbDataReader oleExcelReader = oleExcelCommand.ExecuteReader();
                while(oleExcelReader.Read()) {
                    RowData<ColumnName, object> rowData = new RowData<ColumnName, object>();
                    int fieldCount = oleExcelReader.VisibleFieldCount;
                    for(int a = 0; a < oleExcelReader.VisibleFieldCount; a++) {
                        string columnName = a.ToString();
                        ExcelColumn namedColumn = columns.Where(col => col.ordinal == a).FirstOrDefault();
                        if(namedColumn != null) {
                            columnName = namedColumn.name;
                        }
                        rowData.Add(columnName, oleExcelReader.GetValue(a));
                    }
                    result.Add(rowData);
                }
                oleExcelReader.Close();
            }

            oleExcelConnection.Close();

            return result;
        }

        public static List<RowData<ColumnName, object>> getDataFromExcel_v2(string filename, ExcelColumn[] columns, string sheetName = null) {
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

        public static dynamic getDataFromJson(string filenameTag) {
            DirectoryInfo d = new DirectoryInfo(GlobalConfig.getJsonSourcesPath());

            FileInfo[] Files = d.GetFiles(filenameTag+"*.json");
            string filename = null;

            foreach(FileInfo file in Files) {
                filename = file.Name;
            }

            if(filename == null) {
                throw new FileNotFoundException();
            }

            string path = GlobalConfig.getJsonSourcesPath() + "\\" + filename;
            if(File.Exists(path)) {
                using(StreamReader r = new StreamReader(path)) {
                    string jsonText = r.ReadToEnd();
                    return JsonSerializer.Deserialize<dynamic>(jsonText);
                }
            } else {
                throw new FileNotFoundException("File " + path + " not found");
            }
        }

        public static string executeCmd(string command, string workingDirectory = "", bool asyncOutput = false) {
            string result = null;

            ProcessStartInfo procStartInfo = new ProcessStartInfo("cmd", "/c " + command);

            if(asyncOutput) {
                procStartInfo.RedirectStandardOutput = true;
                procStartInfo.RedirectStandardError = true;
            }
            procStartInfo.UseShellExecute = false;
            procStartInfo.WorkingDirectory = workingDirectory;

            // wrap IDisposable into using (in order to release hProcess) 
            using(Process process = new Process()) {
                process.StartInfo = procStartInfo;

                if(asyncOutput) {
                    process.ErrorDataReceived += new DataReceivedEventHandler(ProcessErrorHandler);
                    process.OutputDataReceived += new DataReceivedEventHandler(ProcessOutputHandler);
                }

                process.Start();

                if(asyncOutput) {
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();
                }

                // Add this: wait until process does its work
                process.WaitForExit();

                if(!asyncOutput) {
                    // and only then read the result
                    result = process.StandardOutput.ReadToEnd();
                }
            }

            return result;
        }

        private static void ProcessErrorHandler(object sendingProcess, DataReceivedEventArgs outLine) {
            //* Do your stuff with the output (write to console/log/StringBuilder)
            if(!String.IsNullOrEmpty(outLine.Data)) {
                MyConsole.Error(outLine.Data);
            }
        }

        private static void ProcessOutputHandler(object sendingProcess, DataReceivedEventArgs outLine) {
            //* Do your stuff with the output (write to console/log/StringBuilder)
            if(!String.IsNullOrEmpty(outLine.Data)) {
                MyConsole.Information(outLine.Data);
            }
        }

        public static string executeCmd(List<string> cmds, string workingDirectory = "") {
            string result = null;

            var psi = new ProcessStartInfo();
            psi.FileName = "cmd.exe";
            psi.RedirectStandardInput = true;
            psi.RedirectStandardOutput = true;
            psi.RedirectStandardError = true;
            psi.UseShellExecute = false;
            psi.WorkingDirectory = workingDirectory;

            using(Process process = new Process()) {
                process.StartInfo = psi;
                process.Start();
                process.OutputDataReceived += (sender, e) => { Console.WriteLine(e.Data); };
                process.ErrorDataReceived += (sender, e) => { Console.WriteLine(e.Data); };
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();
                using(StreamWriter sw = process.StandardInput) {
                    foreach(var cmd in cmds) {
                        sw.WriteLine(cmd);
                    }
                }
                process.WaitForExit();

                result = process.StandardOutput.ReadToEnd();
            }

            return result;
        }
    }
}
