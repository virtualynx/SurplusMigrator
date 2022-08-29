using System;

namespace SurplusMigrator.Models
{
  class DbInsertFail {
        public static string DB_FAIL_SEVERITY_WARNING = "WARNING";
        public static string DB_FAIL_SEVERITY_ERROR = "ERROR";
        public static string DB_FAIL_TYPE_DUPLICATE = "DUPLICATE";
        public static string DB_FAIL_TYPE_NOTNULL_VIOLATION = "NOTNULL_VIOLATION";
        public static string DB_FAIL_TYPE_FOREIGNKEY_VIOLATION = "FOREIGNKEY_VIOLATION";

        public Exception exception = null;
        public string info;
        public string severity;
        public string type;
        public string loggedInFilename;
    }
}
