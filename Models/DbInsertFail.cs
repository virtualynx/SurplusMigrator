using System;
using System.Collections.Generic;

namespace SurplusMigrator.Models
{
  class DbInsertFail {
        public static string DB_FAIL_SEVERITY_WARNING = "WARNING";
        public static string DB_FAIL_SEVERITY_ERROR = "ERROR";

        public Exception exception = null;
        public string info;
        public string severity;
    }
}
