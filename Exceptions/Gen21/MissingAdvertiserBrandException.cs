using System;
using System.Text.Json;

namespace SurplusMigrator.Exceptions.Gen21 {
    internal class MissingAdvertiserBrandException : Exception {
        public MissingAdvertiserBrandException() : base() {
        }
        public MissingAdvertiserBrandException(string message) : base(message) {
            info.message = message;
        }
        public MissingAdvertiserBrandException(string message, MissingAdvertiserBrandException sourceException) : base(message) {
            info.message = message;
            info = sourceException.info;
        }

        public MissingAdvertiserBrandExceptionInfo info { get; set; } = new MissingAdvertiserBrandExceptionInfo();
    }

    class MissingAdvertiserBrandExceptionInfo {
        public string message { get; set; }
        public int advertiserId { get; set; }
        public string[] masterTrafficadvertiserNames { get; set; }
        public string masterAdvertiserName { get; set; }
        public Advertiser[] advertiserPossibleResults { get; set; }

        public int advertiserBrandId { get; set; }
        public string[] masterTrafficbrandNames { get; set; }
        public string masterAdvertiserbrandName { get; set; }
        public Brand[] brandPossibleResults { get; set; }

        public override string ToString() {
            return JsonSerializer.Serialize(this);
        }
    }
    class Advertiser {
        public string id { get; set; }
        public string name { get; set; }

        public override string ToString() {
            return JsonSerializer.Serialize(this);
        }
    }

    class Brand {
        public string id { get; set; }
        public string name { get; set; }

        public override string ToString() {
            return JsonSerializer.Serialize(this);
        }
    }
}
