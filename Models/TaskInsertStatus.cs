using System;
using System.Collections.Generic;

namespace SurplusMigrator.Models
{
  class TaskInsertStatus {
        public List<DbInsertFail> failures { get; set; } = new List<DbInsertFail>();
        public int successCount = 0;
    }
}
