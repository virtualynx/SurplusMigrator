using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace SurplusMigrator.Tasks {
    class _BackupSurplus : _BaseTask {
        public _BackupSurplus(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        protected override void onFinished() {
            DbLoginInfo loginInfo = connections.First(a => a.GetDbLoginInfo().name == "surplus").GetDbLoginInfo();

            //string strCmdText = @"/C pg_dump -Fc --dbname=postgresql://[username]:[password]@[host]:[port]/[dbname]?currentSchema=[schema] > [filename]"
            //    .Replace("[username]", loginInfo.username)
            //    .Replace("[password]", loginInfo.password)
            //    .Replace("[host]", loginInfo.host)
            //    .Replace("[port]", loginInfo.port.ToString())
            //    .Replace("[dbname]", loginInfo.dbname)
            //    .Replace("[schema]", loginInfo.schema)
            //    .Replace("[filename]", "surplus_" + DateTime.Now.ToString("yyyyMMdd_HHmmss") + ".gzip")
            //;

            string strCmdTemplate = @"/C pg_dump --host [host] --port [port] --username ""[username]"" --role ""[username]"" --format tar --encoding UTF8 --verbose --exclude-table ""[schema].view_*"" --file ""[filename]"" --schema ""[schema]"" ""[dbname]"""
                .Replace("[username]", loginInfo.username)
                .Replace("[password]", loginInfo.password)
                .Replace("[host]", loginInfo.host)
                .Replace("[port]", loginInfo.port.ToString())
                .Replace("[dbname]", loginInfo.dbname)
                .Replace("[schema]", loginInfo.schema)
            ;

            DateTime today = DateTime.MinValue;
            DateTime scheduledTime = DateTime.MinValue;
            bool alreadyRunToday = false;
            string scheduledHour = getOptions("time");
            while(true) {
                DateTime now = DateTime.Now;
                if(now.Date > today.Date) {
                    today = now;
                    alreadyRunToday = false;
                    scheduledTime = DateTime.ParseExact(now.ToString("yyyy-MM-dd") + " " + scheduledHour, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                }
                if(!alreadyRunToday && now >= scheduledTime) {
                    Console.WriteLine();
                    MyConsole.Information("Backup database " + loginInfo.dbname + "(schema: "+loginInfo.schema+") started ...");

                    string strCmdText = strCmdTemplate.Replace("[filename]", "surplus_" + DateTime.Now.ToString("yyyyMMdd_HHmmss") + ".backup");

                    //System.Diagnostics.Process.Start("CMD.exe", strCmdText);
                    ProcessStartInfo cmdsi = new ProcessStartInfo("cmd.exe");
                    cmdsi.EnvironmentVariables["PGPASSWORD"] = loginInfo.password;
                    cmdsi.UseShellExecute = false;
                    cmdsi.Arguments = strCmdText;
                    Process cmd = Process.Start(cmdsi);
                    cmd.WaitForExit();

                    alreadyRunToday = true;
                    MyConsole.Information("Backup database " + loginInfo.dbname + "(schema: " + loginInfo.schema + ") finished.");
                } else {
                    MyConsole.EraseLine();
                    TimeSpan remaining;
                    if(alreadyRunToday) {
                        DateTime nextSchedule = scheduledTime.AddDays(1);
                        remaining = nextSchedule - now;
                    } else {
                        remaining = scheduledTime - now;
                    }
                    MyConsole.Write(remaining.ToString() + " before next backup");
                }
                Thread.Sleep(5000);
            }
        }
    }
}
