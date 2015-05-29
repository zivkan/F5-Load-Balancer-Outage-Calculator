using System;
using System.Net;

namespace OutageCalculator.Model
{
    class F5LogRow
    {
        public DateTime TrapTime { get; set; }
        public IPAddress IpAddress { get; set; }
        public string HostName { get; set; }
        public string CommunityString { get; set; }
        public string TrapType { get; set; }
        public string TrapDetails { get; set; }
        public string Member { get; set; }
        public int RowNumber { get; set; }
    }
}