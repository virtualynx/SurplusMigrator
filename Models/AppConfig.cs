namespace SurplusMigrator.Models
{
  class AppConfig
    {
        public AppConfig(){

        }

        public DbLoginInfo[] databases { get; set; }
        public ExcludedTables excluded_tables { get; set; }
        public string[] truncated_tables { get; set; }
    }
}
