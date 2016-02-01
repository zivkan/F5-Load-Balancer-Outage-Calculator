using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using ClosedXML.Excel;
using OutageCalculator.Model;

namespace OutageCalculator
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("{0} filename", Path.GetFileName(Process.GetCurrentProcess().MainModule.FileName));
            }

            XLWorkbook xlsFile = new XLWorkbook(args[0]);
            var sheet = xlsFile.Worksheet(1);
            var rows = sheet.RowsUsed().Skip(1);

            var f5logs = GetLogRows(rows);
            var events = f5logs.Select(ConvertToEventInfo);
            var eventsPerHost =
                events.GroupBy(e => e.Host)
                    .ToDictionary<IGrouping<IPAddress, EventInfo>, IPAddress, IEnumerable<EventInfo>>(e => e.Key, e => e);
            List<KeyValuePair<IPAddress, IEnumerable<DowntimeEvent>>> downTimePerHost = new List<KeyValuePair<IPAddress, IEnumerable<DowntimeEvent>>>();
            foreach (var hostGrouping in eventsPerHost)
            {
                var eventsWithImputation = ImputeMissingEvents(hostGrouping.Value).ToList();
                var downTime = CalculateHostDowntime(eventsWithImputation);
                downTimePerHost.Add(new KeyValuePair<IPAddress, IEnumerable<DowntimeEvent>>(hostGrouping.Key, downTime));
            }
            var downtimeEvents = FindDowntimeEvents(downTimePerHost);

            Console.WriteLine("\"{0}\",\"{1}\",\"{2}\"", "Start", "End", "Duration");
            foreach (var downtime in downtimeEvents)
            {
                Console.WriteLine("\"{0:yyyy-MM-dd hh:mm:ss}\",\"{1:yyyy-MM-dd hh:mm:ss}\",\"{2}\"", downtime.Start, downtime.End,
                    downtime.End - downtime.Start);
            }
        }

        private static IEnumerable<EventInfo> ImputeMissingEvents(IEnumerable<EventInfo> events)
        {
            var sortedEvents = events.ToList();
            sortedEvents.Sort((event1, event2) => event1.When.CompareTo(event2.When));

            var enumerator = sortedEvents.GetEnumerator();
            if (!enumerator.MoveNext())
            {
                yield break;
            }

            yield return enumerator.Current;
            var isUp = enumerator.Current.Up;

            while (enumerator.MoveNext())
            {
                if (isUp == enumerator.Current.Up)
                { 
                    // impute
                    var startTime = enumerator.Current.When;
                    var timeSpanSegments = enumerator.Current.timeSpanMessage.Split(':');
                    foreach (var segment in timeSpanSegments)
                    {
                        if (segment.EndsWith("hr", StringComparison.CurrentCultureIgnoreCase))
                        {
                            var duration = int.Parse(segment.Substring(0, segment.Length - 2));
                            startTime = startTime.AddHours(-duration);
                        }
                        else if (segment.EndsWith("hrs", StringComparison.CurrentCultureIgnoreCase))
                        {
                            var duration = int.Parse(segment.Substring(0, segment.Length - 3));
                            startTime = startTime.AddHours(-duration);
                        }
                        else if (segment.EndsWith("min", StringComparison.OrdinalIgnoreCase))
                        {
                            var duration = int.Parse(segment.Substring(0, segment.Length - 3));
                            startTime = startTime.AddMinutes(-duration);
                        }
                        else if (segment.EndsWith("mins", StringComparison.OrdinalIgnoreCase))
                        {
                            var duration = int.Parse(segment.Substring(0, segment.Length - 4));
                            startTime = startTime.AddMinutes(-duration);
                        }
                        else if (segment.EndsWith("sec", StringComparison.OrdinalIgnoreCase))
                        {
                            var duration = int.Parse(segment.Substring(0, segment.Length - 3));
                            startTime = startTime.AddSeconds(-duration);
                        }
                        else if (segment.EndsWith("secs", StringComparison.OrdinalIgnoreCase))
                        {
                            var duration = int.Parse(segment.Substring(0, segment.Length - 4));
                            startTime = startTime.AddSeconds(-duration);
                        }
                        else
                        {
                            throw new Exception("Unknown time segment '" + segment + "'");
                        }
                    }

                    var imputed = new EventInfo()
                    {
                        timeSpanMessage = "imputed",
                        Up = !isUp,
                        When = startTime,
                        Host = enumerator.Current.Host
                    };
                    var upOrDown = imputed.Up ? "up" : "down";
                    Console.Error.WriteLine("Imputed " + upOrDown + " event for " + imputed.Host + " at " +
                                            imputed.When.ToString("yyyy-MM-dd hh:mm:ss"));
                    yield return imputed;
                }
                yield return enumerator.Current;
                isUp = enumerator.Current.Up;
            }
        }

        private static IEnumerable<DowntimeEvent> FindDowntimeEvents(List<KeyValuePair<IPAddress, IEnumerable<DowntimeEvent>>> downtimePerHost)
        {
            var events = downtimePerHost.Select(e => new {Host = e.Key, Enumerator = e.Value.GetEnumerator()}).ToList();

            // get first event for each host
            foreach (var e in events)
            {
                if (!e.Enumerator.MoveNext())
                {
                    Console.WriteLine("Host {0} had 100% uptime!", e.Host);
                    yield break;
                }
            }

            for (;;)
            {
                int earliestEndIndex = 0;
                DateTime earliestEndTime = events[0].Enumerator.Current.End;

                // find earliest end of downtime
                for (int i = 1; i < events.Count; i++)
                {
                    var endTime = events[i].Enumerator.Current.End;
                    if (endTime < earliestEndTime)
                    {
                        earliestEndTime = endTime;
                        earliestEndIndex = i;
                    }
                }

                // check if all servers were down at same time
                if (events.All(e => e.Enumerator.Current.Start < earliestEndTime))
                {
                    // find latest start of downtime
                    var start = events.Select(e => e.Enumerator.Current.Start).Max();
                    yield return new DowntimeEvent()
                    {
                        Start = start,
                        End = earliestEndTime
                    };
                }

                // prepare for next loop iteration
                if (!events[earliestEndIndex].Enumerator.MoveNext())
                {
                    // even if other servers were down, at least this one was up for the rest of the reported time period
                    yield break;
                }
            }
        }

        private static IEnumerable<DowntimeEvent> CalculateHostDowntime(IEnumerable<EventInfo> arg)
        {
            var events = arg.ToList();
            events.Sort((e1, e2) => e1.When.CompareTo(e2.When));

            var enumerator = arg.GetEnumerator();

            if (!enumerator.MoveNext())
            {
                // no downtime for host!!
                // but how was the grouping created?
                yield break;
            }

            bool up = enumerator.Current.Up;
            DateTime downtimeStart = enumerator.Current.When;
            if (up)
            {
                yield return new DowntimeEvent()
                {
                    Start = DateTime.MinValue,
                    End = downtimeStart
                };
            }

            while (enumerator.MoveNext())
            {
                if (enumerator.Current.Up == up)
                {
                    var upSting = up ? "up" : "down";
                    throw new Exception(String.Format("Got two {0} events in a row.", upSting));
                }
                if (!enumerator.Current.Up)
                {
                    up = enumerator.Current.Up;
                    downtimeStart = enumerator.Current.When;
                }
                else
                {
                    yield return new DowntimeEvent()
                    {
                        Start = downtimeStart,
                        End = enumerator.Current.When
                    };
                    up = enumerator.Current.Up;
                }
            }

            if (!up)
            {
                yield return new DowntimeEvent()
                {
                    Start = downtimeStart,
                    End = DateTime.MaxValue
                };
            }
        }

        static IEnumerable<F5LogRow> GetLogRows(IEnumerable<IXLRow> rows)
        {
            var expectedDataTypes = new[]
            {
                XLCellValues.DateTime, XLCellValues.Text, XLCellValues.Text, XLCellValues.Text, XLCellValues.Text,
                XLCellValues.Text,XLCellValues.Text, 
            };
            foreach (var row in rows)
            {
                var data = row.Cells().ToList();
                if (data.Count != expectedDataTypes.Length)
                {
                    throw new Exception(String.Format("Expected {0} columns, but found {1} on row {2}",
                        expectedDataTypes.Length, data.Count, row.RowNumber()));
                }

                for (var column = 0; column < expectedDataTypes.Length; column++)
                {
                    if (data[column].DataType != expectedDataTypes[column])
                    {
                        throw new Exception(
                            string.Format("Expected DateTime value in column {0}, but found type {1} on row {2}",
                                (char) ('A' + column), data[0].DataType, row.RowNumber()));
                    }
                }

                DateTime trapTime = (DateTime) data[0].Value;

                IPAddress ipAddress;
                if (!IPAddress.TryParse((string)data[1].Value, out ipAddress))
                {
                    throw new Exception(String.Format("Could not parse '{0}' as an IP address on row {1}",
                        (string) data[1].Value, row.RowNumber()));
                }

                yield return new F5LogRow()
                {
                    TrapTime = trapTime,
                    IpAddress = ipAddress,
                    HostName = (string) data[2].Value,
                    CommunityString = (string) data[3].Value,
                    TrapType = (string) data[4].Value,
                    TrapDetails = (string) data[5].Value,
                    Member = (string) data[6].Value,
                    RowNumber = row.RowNumber()
                };
            }
        }

        static EventInfo ConvertToEventInfo(F5LogRow row)
        {
            bool up = false;
            if (row.TrapType.Contains("ServiceDown"))
            {
                up = false;
            }
            else if (row.TrapType.Contains("ServiceUp"))
            {
                up = true;
            }
            else
            {
                throw new Exception(String.Format("Could not determine if '{0}' means the service is up or down", row.TrapType));
            }

            const string ipStartString = "member /Common/";
            var ipStartPos = row.TrapDetails.IndexOf(ipStartString, StringComparison.InvariantCultureIgnoreCase);
            var ipEndString = ":80";
            var ipEndPos = row.TrapDetails.IndexOf(ipEndString, StringComparison.InvariantCultureIgnoreCase);

            var ipAddressString = row.TrapDetails.Substring(ipStartPos + ipStartString.Length,
                ipEndPos - ipStartPos - ipStartString.Length);
            IPAddress ipAddress;
            if (!IPAddress.TryParse(ipAddressString, out ipAddress))
            {
                throw new Exception(String.Format("Could not parse '{0}' as an IP address on line {1}", ipAddressString,
                    row.RowNumber));
            }

            const string forString = " for ";
            var timeSpanMessageStart = row.TrapDetails.IndexOf(forString, StringComparison.OrdinalIgnoreCase) + forString.Length;
            var timeSpanMessageEnd = row.TrapDetails.IndexOf(" ]", timeSpanMessageStart, StringComparison.OrdinalIgnoreCase);
            var timeSpanMessage = row.TrapDetails.Substring(timeSpanMessageStart, timeSpanMessageEnd - timeSpanMessageStart);

            return new EventInfo()
            {
                Host = ipAddress,
                Up = up,
                When = row.TrapTime,
                timeSpanMessage = timeSpanMessage
            };
        }

    }
}
