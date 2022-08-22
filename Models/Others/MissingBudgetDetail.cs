using System.Collections.Generic;

namespace SurplusMigrator.Models.Others
{
  class MissingBudgetDetail {
        public MissingBudgetDetail(){

        }

        public long budgetdetil_id { get; set; }
        public List<JournalDetail> journalDetails { get; set; } = new List<JournalDetail>();
    }
}
