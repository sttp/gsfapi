//******************************************************************************************************
//  InteropTest.cpp - Gbtc
//
//  Copyright © 2019, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
//  file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  11/22/2019 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

using GSF;
using GSF.TimeSeries;
using sttp;
using System;
using System.Collections.Generic;
using System.IO;

namespace InteropTest
{
    class Program
    {
        private static readonly object s_coutLock = new object();
        private static StreamWriter s_export;
        private static ulong s_processCount;
        private static bool s_ready;

        private static void StatusMessage(string message)
        {
            // Calls can come from multiple threads, so we impose a simple lock before write to console
            lock (s_coutLock)
                Console.WriteLine(message);
        }

        private static void ErrorMessage(string message)
        {
            // Calls can come from multiple threads, so we impose a simple lock before write to console
            lock (s_coutLock)
                Console.Error.WriteLine(message);
        }

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("    InteropTest HOSTNAME PORT");
                return;
            }

            // Get hostname and port.
            string hostname = args[0];
            
            if (!ushort.TryParse(args[1], out ushort port))
            {
                Console.Error.WriteLine($"Could not parse \"{args[1]}\" as a valid port.");
                return;
            }

            // Initialize the subscribers, maintaining the life-time of SubscriberHandler instances within main
            DataSubscriber subscriber = new DataSubscriber();
            subscriber.StatusMessage += (_, e) => StatusMessage(e.Argument);
            subscriber.ProcessException += (_, e) => ErrorMessage(e.Argument.Message);
            subscriber.ConnectionEstablished += subscriber_ConnectionEstablished;
            subscriber.ConnectionTerminated += subscriber_ConnectionTerminated;
            subscriber.NewMeasurements += subscriber_NewMeasurements;
            subscriber.ConnectionString = $"server={hostname}:{port}";
            subscriber.CompressionModes |= CompressionModes.TSSC;
            subscriber.Initialize();
            subscriber.Start();

            Console.ReadLine();

            subscriber.Stop();
        }

        private static void subscriber_NewMeasurements(object sender, EventArgs<ICollection<IMeasurement>> e)
        {
            const ulong interval = 30 * 2;
            const ulong exportCount = 500;
            DataSubscriber subscriber = sender as DataSubscriber;
            ICollection<IMeasurement> measurements = e.Argument;
            ulong measurementCount = (ulong)measurements.Count;
            bool showMessage = (s_processCount + measurementCount >= (s_processCount / interval + 1) * interval);

            if (s_processCount >= exportCount)
                return;

            if (showMessage)
                StatusMessage($"{subscriber?.ProcessedMeasurements} measurements received so far...{Environment.NewLine}");

            // Process measurements
            foreach (IMeasurement measurement in measurements)
            {
                DateTime timestamp = measurement.Timestamp;

                if (!s_ready)
                {
                    // Start export at top of second
                    if (timestamp.Millisecond == 0)
                        s_ready = true;
                    else
                        continue;

                    StatusMessage($"Export started for measurement {measurement.Key.SignalID} timestamp at {measurement.Timestamp:yyyy-MM-dd HH:mm:ss.ffffff}");

                    s_export.WriteLine($"Export for measurement {measurement.Key.SignalID}{Environment.NewLine}");
                    s_export.WriteLine("Timestamp,Value,Flags");
                }

                s_export.WriteLine($"{measurement.Timestamp:yyyy-MM-dd HH:mm:ss.ffffff},{measurement.Value:0.0000000000},{(int)measurement.StateFlags}");

                s_processCount++;

                if (s_processCount >= exportCount)
                {
                    subscriber?.Stop();
                    break;
                }
            }
        }

        private static void subscriber_ConnectionEstablished(object sender, EventArgs e)
        {
            DataSubscriber subscriber = sender as DataSubscriber;

            StatusMessage("Connection established.");
            s_export = File.CreateText("InteropSubscriber-gsf.csv");

            subscriber?.Subscribe(new SubscriptionInfo
            {
                FilterExpression = "FILTER ActiveMeasurements WHERE SignalType='VPHM' ORDER BY ID"
            });
        }

        private static void subscriber_ConnectionTerminated(object sender, EventArgs e)
        {
            StatusMessage("Connection terminated.");
            s_export?.Close();
            s_export = null;
        }
    }
}
