namespace SurplusMigrator.Models
{
  class DbLoginInfo
    {
        public DbLoginInfo(){

        }

        public string name { get; set; }
        public string host{get;set;}
        public int port{get;set;}
        public string username{get;set;}
        public string password{get;set;}
        public string dbname{get;set;}
        public string schema{get;set; }
        public string type { get; set; }
    }
}
