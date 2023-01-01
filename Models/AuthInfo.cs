using System.Text.Json;

namespace SurplusMigrator.Models {
    class AuthInfo {
        public string Id{get;set; } = null;
        public string NIK { get; set; } = null;
        public string FullName { get; set; } = null;
        public string Department_Id { get; set; } = null;
        public int Occupation_Id { get; set; } = 0;
        public int ModuleGroup_Id { get; set; } = 0;

        public override string ToString() {
            return JsonSerializer.Serialize(this);
        }

    }
}
