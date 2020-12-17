//******************************************************************************************************
//  SubscriptionInfo.cs - Gbtc
//
//  Copyright © 2012, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may
//  not use this file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://www.opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  04/25/2012 - Stephen C. Wills
//       Generated original version of source code.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//
//******************************************************************************************************

using GSF;
using System;

namespace sttp
{
    /// <summary>
    /// Configuration object for data subscriptions.
    /// </summary>
    public sealed class SubscriptionInfo
    {
        #region [ Constructors ]

        /// <summary>
        /// Creates a new instance of the <see cref="SubscriptionInfo"/> class.
        /// </summary>
        public SubscriptionInfo(bool throttled = false) => Throttled = throttled;

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets or sets the filter expression used to define which
        /// measurements are being requested by the subscriber.
        /// </summary>
        public string FilterExpression { get; set; }

        /// <summary>
        /// Gets or sets the flag that determines whether to use the
        /// compact measurement format or the full measurement format
        /// for transmitting measurements to the subscriber.
        /// </summary>
        public bool UseCompactMeasurementFormat { get; set; } = true;

        /// <summary>
        /// Gets or sets the flag that determines whether the subscriber
        /// is requesting its data over a separate UDP data channel.
        /// </summary>
        public bool UdpDataChannel { get; set; }

        /// <summary>
        /// Gets or sets the port number that the UDP data channel binds to.
        /// This value is only used when the subscriber requests a separate
        /// UDP data channel.
        /// </summary>
        public int DataChannelLocalPort { get; set; } = 9500;

        /// <summary>
        /// Gets or sets the allowed past time deviation
        /// tolerance in seconds (can be sub-second).
        /// </summary>
        public double LagTime { get; set; } = 10.0;

        /// <summary>
        /// Gets or sets the allowed future time deviation
        /// tolerance, in seconds (can be sub-second).
        /// </summary>
        public double LeadTime { get; set; } = 5.0;

        /// <summary>
        /// Gets or sets the flag that determines whether the server's
        /// local clock is used as real-time. If false, the timestamps
        /// of the measurements will be used as real-time.
        /// </summary>
        public bool UseLocalClockAsRealTime { get; set; }

        /// <summary>
        /// Gets or sets the flag that determines whether measurement timestamps use
        /// millisecond resolution. If false, they will use <see cref="Ticks"/> resolution.
        /// </summary>
        /// <remarks>
        /// This flag determines the size of the timestamps transmitted as part of
        /// the compact measurement format when the server is using base time offsets.
        /// </remarks>
        public bool UseMillisecondResolution { get; set; }

        /// <summary>
        /// Gets or sets the flag that determines whether to request that measurements
        /// sent to the subscriber should be filtered by the publisher prior to sending them.
        /// </summary>
        public bool RequestNaNValueFilter { get; set; }

        /// <summary>
        /// Gets or sets the start time of the requested
        /// temporal session for streaming historic data.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When the <see cref="StartTime"/> or <see cref="StopTime"/> temporal processing constraints are defined (i.e., not <c>null</c>), this
        /// specifies the start and stop time over which the subscriber session will process data. Passing in <c>null</c> for the <see cref="StartTime"/>
        /// and <see cref="StopTime"/> specifies the subscriber session will process data in standard, i.e., real-time, operation.
        /// </para>
        /// 
        /// <para>
        /// Both the <see cref="StartTime"/> and <see cref="StopTime"/> parameters can be specified in one of the
        /// following formats:
        /// <list type="table">
        ///     <listheader>
        ///         <term>Time Format</term>
        ///         <description>Format Description</description>
        ///     </listheader>
        ///     <item>
        ///         <term>12-30-2000 23:59:59.033</term>
        ///         <description>Absolute date and time.</description>
        ///     </item>
        ///     <item>
        ///         <term>*</term>
        ///         <description>Evaluates to <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        ///     <item>
        ///         <term>*-20s</term>
        ///         <description>Evaluates to 20 seconds before <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        ///     <item>
        ///         <term>*-10m</term>
        ///         <description>Evaluates to 10 minutes before <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        ///     <item>
        ///         <term>*-1h</term>
        ///         <description>Evaluates to 1 hour before <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        ///     <item>
        ///         <term>*-1d</term>
        ///         <description>Evaluates to 1 day before <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        /// </list>
        /// </para>
        /// </remarks>
        public string StartTime { get; set; }

        /// <summary>
        /// Gets or sets the stop time of the requested
        /// temporal session for streaming historic data.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When the <see cref="StartTime"/> or <see cref="StopTime"/> temporal processing constraints are defined (i.e., not <c>null</c>), this
        /// specifies the start and stop time over which the subscriber session will process data. Passing in <c>null</c> for the <see cref="StartTime"/>
        /// and <see cref="StopTime"/> specifies the subscriber session will process data in standard, i.e., real-time, operation.
        /// </para>
        /// 
        /// <para>
        /// Both the <see cref="StartTime"/> and <see cref="StopTime"/> parameters can be specified in one of the
        /// following formats:
        /// <list type="table">
        ///     <listheader>
        ///         <term>Time Format</term>
        ///         <description>Format Description</description>
        ///     </listheader>
        ///     <item>
        ///         <term>12-30-2000 23:59:59.033</term>
        ///         <description>Absolute date and time.</description>
        ///     </item>
        ///     <item>
        ///         <term>*</term>
        ///         <description>Evaluates to <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        ///     <item>
        ///         <term>*-20s</term>
        ///         <description>Evaluates to 20 seconds before <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        ///     <item>
        ///         <term>*-10m</term>
        ///         <description>Evaluates to 10 minutes before <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        ///     <item>
        ///         <term>*-1h</term>
        ///         <description>Evaluates to 1 hour before <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        ///     <item>
        ///         <term>*-1d</term>
        ///         <description>Evaluates to 1 day before <see cref="DateTime.UtcNow"/>.</description>
        ///     </item>
        /// </list>
        /// </para>
        /// </remarks>
        public string StopTime { get; set; }

        /// <summary>
        /// Gets or sets the additional constraint parameters
        /// supplied to temporal adapters in a temporal session.
        /// </summary>
        public string ConstraintParameters { get; set; }

        /// <summary>
        /// Gets or sets the processing interval requested by the subscriber.
        /// A value of <c>-1</c> indicates the default processing interval.
        /// A value of <c>0</c> indicates data will be processed as fast as
        /// possible.
        /// </summary>
        /// <remarks>
        /// With the exception of the values of -1 and 0, the <see cref="ProcessingInterval"/> value specifies the desired historical playback data
        /// processing interval in milliseconds. This is basically a delay, or timer interval, over which to process data. Setting this value to -1 means
        /// to use the default processing interval while setting the value to 0 means to process data as fast as possible.
        /// </remarks>
        public int ProcessingInterval { get; set; } = -1;

        /// <summary>
        /// Gets or sets the flag that determines whether
        /// to request that the subscription be throttled.
        /// </summary>
        public bool Throttled { get; set; }

        /// <summary>
        /// Gets or sets the interval at which data should be
        /// published when using a throttled subscription.
        /// </summary>
        public double PublishInterval { get; set; } = -1;

        /// <summary>
        /// Gets or sets the flag that determines whether timestamps are
        /// included in the data sent from the publisher. This value is
        /// ignored if the data is remotely synchronized.
        /// </summary>
        public bool IncludeTime { get; set; } = true;

        /// <summary>
        /// Gets or sets the additional connection string parameters to
        /// be applied to the connection string sent to the publisher
        /// during subscription.
        /// </summary>
        public string ExtraConnectionStringParameters { get; set; }

        #endregion
    }
}
