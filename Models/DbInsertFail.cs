using System;
using System.Collections.Generic;

namespace SurplusMigrator.Models
{
  class DbInsertFail {
        public Exception exception;
        public string info;
        public string severity;
        public bool success;
    }
}
