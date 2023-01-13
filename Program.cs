using Serilog;
using Serilog.Events;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;

namespace SurplusMigrator {
    internal class Program {
        static void Main(string[] args) {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            MyConsole.stopwatch = stopwatch;

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.File(
                    "log.txt", 
                    //outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}",
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    rollingInterval: RollingInterval.Day,
                    rollOnFileSizeLimit: true,
                    fileSizeLimitBytes: 50000000
                )
                //.WriteTo.Console(restrictedToMinimumLevel: LogEventLevel.Information)
                .CreateLogger();

            MyConsole.Information("Reading configuration at " + Misc.FILEPATH_CONFIG);
            AppConfig config = null;

            try {
                using(StreamReader r = new StreamReader(Misc.FILEPATH_CONFIG)) {
                    string json = r.ReadToEnd();
                    config = JsonSerializer.Deserialize<AppConfig>(json);
                }
                //MyConsole.Information("Configuration loaded : " + JsonSerializer.Serialize(config) + "\n");
                MyConsole.Information("Configuration loaded !\n");
            } catch(Exception e) {
                MyConsole.Error(e, "Error upon loading config file");
                return;
            }
            GlobalConfig.loadConfig(config);

            List<DbConnection_> connList = new List<DbConnection_>();

            foreach(DbLoginInfo loginInfo in config.databases) {
                connList.Add(new DbConnection_(loginInfo));
            }

            DbConnection_[] connections = connList.ToArray();

            try {
                if(config.pre_queries_path != null) {
                    QueryExecutor qe = new QueryExecutor(connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault());
                    qe.execute(config.pre_queries_path);
                }

                IdRemapper.loadMap();

                OrderedJob[] jobs = getAllJob(config);

                foreach(var job in jobs) {
                    var taskType = Type.GetType("SurplusMigrator.Tasks." + job.name);
                    if(taskType != null) {
                        var instantiatedObject = Activator.CreateInstance(taskType, new object[] { connections }) as _BaseTask;
                        instantiatedObject.run(job.cascade);
                    } else {
                        MyConsole.Warning("Task with name " + job.name + " cannot be found");
                    }
                }
            } catch(Exception e) {
                MyConsole.Error(e, "Program stopped abnormally due to some error");
            } finally {
                IdRemapper.saveMap();
                foreach(DbConnection_ con in connections) {
                    con.Close();
                }
            }

            stopwatch.Stop();
            MyConsole.Information("Program finished in " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds));

            Console.WriteLine("\n\nPress any key to exit ...");
            Console.ReadLine();
        }

        private static OrderedJob[] getAllJob(AppConfig config) {
            List<OrderedJob> orderedJobs = new List<OrderedJob>();

            if(config.job_playlist.Length > 0) {
                int order = 0;
                foreach(var job in config.job_playlist) {
                    job.order = order++;
                }

                return config.job_playlist;
            } else {
                var taskList = Assembly.GetExecutingAssembly().GetTypes()
                .Where(a =>
                    a.Namespace == "SurplusMigrator.Tasks"
                    && !a.Name.StartsWith("<>")
                    && !a.Name.StartsWith("_")
                )
                .ToList();

                foreach(var task in taskList) {
                    orderedJobs.Add(new OrderedJob() {
                        name = task.Name,
                        order = getJobOrder(config, task.Name),
                        cascade = true
                    });
                }

                OrderedJob[] orderedJobsArr = orderedJobs.ToArray();

                Array.Sort(
                    orderedJobsArr,
                    delegate (OrderedJob x, OrderedJob y) { return x.order - y.order; }
                );

                return orderedJobsArr;
            }
        }

        private static int getJobOrder(AppConfig config, string taskName) {
            for(int a = 0; a < config.job_order.Length; a++) {
                if(config.job_order[a] == taskName) {
                    return a;
                }
            }

            return Int32.MaxValue;
        }
    }
}
