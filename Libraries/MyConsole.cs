using Serilog;
using System;
using System.Diagnostics;

namespace SurplusMigrator.Libraries {
    class MyConsole {
        public static Stopwatch stopwatch = null;

        public static void EraseLine() {
            int currentLineCursor = Console.CursorTop;
            Console.SetCursorPosition(0, Console.CursorTop);
            for(int i = 0; i < Console.WindowWidth; i++)
                Console.Write(" ");
            Console.SetCursorPosition(0, currentLineCursor);
        }
        public static void Write(string str) {
            string elapsedTag = "";
            if(stopwatch != null) {
                elapsedTag = "[" + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + "    ] ";
            }
            Console.Write(elapsedTag + str);
        }
        public static void WriteLine(string str) {
            string elapsedTag = "";
            if(stopwatch != null) {
                elapsedTag = "[" + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + "    ] ";
            }
            Console.WriteLine(elapsedTag + str);
        }
        public static void Information(string str) {
            string elapsedTag = "";
            if(stopwatch != null) {
                elapsedTag = "[" + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + " INF] ";
            }
            Console.WriteLine(elapsedTag + str);
            Log.Logger.Information(str);
        }
        public static void Warning(string str) {
            string elapsedTag = "";
            if(stopwatch != null) {
                elapsedTag = "[" + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + " WRN] ";
            }
            Console.WriteLine(elapsedTag + str);
            Log.Logger.Warning(str);
        }
        public static void Error(string str) {
            string elapsedTag = "";
            if(stopwatch != null) {
                elapsedTag = "[" + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + " ERR] ";
            }
            Console.WriteLine(elapsedTag + str);
            Log.Logger.Error(str);
        }
        public static void Error(Exception e, string str) {
            string elapsedTag = "";
            if(stopwatch != null) {
                elapsedTag = "[" + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + " ERR) ";
            }
            Console.WriteLine(elapsedTag + str);
            //Console.WriteLine(e.ToString());
            Console.WriteLine(e.StackTrace);
            Log.Logger.Error(e, str);
        }
    }
}
