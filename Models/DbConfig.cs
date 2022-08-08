namespace SurplusMigrator.Models
{
  class DbConfig
    {
        public DbConfig(){

        }

        public DbLoginInfo source{get;set;}
        public DbLoginInfo destination{get;set;}
    }
}
