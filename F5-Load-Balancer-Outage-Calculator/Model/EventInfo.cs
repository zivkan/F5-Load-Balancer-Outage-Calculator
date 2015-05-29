using System;
using System.Net;

namespace OutageCalculator.Model
{
    class EventInfo
    {
        public IPAddress Host { get; set; }
        public bool Up { get; set; }
        public DateTime When { get; set; }
    }
}