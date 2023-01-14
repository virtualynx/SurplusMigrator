namespace SurplusMigrator.Models
{
  class ExcelColumn {
        public ExcelColumn(){

        }

        public string name { get; set; }
        /// <summary>
        /// 1-based ordinal number
        /// </summary>
        public int ordinal { get; set; }
    }
}
