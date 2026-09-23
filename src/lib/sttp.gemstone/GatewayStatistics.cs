//******************************************************************************************************
//  GatewayStatistics.cs - Gbtc
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
//  03/09/2012 - Stephen C. Wills
//       Generated original version of source code.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//  11/09/2023 - Lillian Gensolin
//       Converted code to .NET core.
//
//******************************************************************************************************

// ReSharper disable UnusedMember.Local
// ReSharper disable UnusedParameter.Local
#pragma warning disable IDE0060 // Remove unused parameter

namespace sttp;

internal static class GatewayStatistics
{
    #region [ Subscriber Statistics ]

    private static double GetSubscriberStatistic_Connected(object source, string arguments)
    {
        return source is DataSubscriber subscriber && subscriber.IsConnected ? 1.0D : 0.0D;
    }

    private static double GetSubscriberStatistic_ProcessedMeasurements(object source, string arguments)
    {
        double statistic = source is DataSubscriber subscriber ? subscriber.ProcessedMeasurements : 0.0D;
        return s_statisticValueCache.GetDifference(source, statistic, nameof(DataSubscriber.ProcessedMeasurements));
    }

    private static double GetSubscriberStatistic_TotalBytesReceived(object source, string arguments)
    {
        double statistic = source is DataSubscriber subscriber ? subscriber.TotalBytesReceived : 0.0D;
        double difference = s_statisticValueCache.GetDifference(source, statistic, nameof(DataSubscriber.TotalBytesReceived));
        return difference < 0.0D ? statistic : difference;
    }

    private static double GetSubscriberStatistic_AuthorizedCount(object source, string arguments)
    {
        Guid[] authorizedSignalIDs = source is DataSubscriber subscriber ? subscriber.GetAuthorizedSignalIDs() : [];
        return authorizedSignalIDs.Length;
    }

    private static double GetSubscriberStatistic_UnauthorizedCount(object source, string arguments)
    {
        Guid[] unauthorizedSignalIDs = source is DataSubscriber subscriber ? subscriber.GetUnauthorizedSignalIDs() : [];
        return unauthorizedSignalIDs.Length;
    }

    private static double GetSubscriberStatistic_LifetimeMeasurements(object source, string arguments)
    {
        return source is DataSubscriber subscriber ? subscriber.LifetimeMeasurements : 0.0D;
    }

    private static double GetSubscriberStatistic_MinimumMeasurementsPerSecond(object source, string arguments)
    {
        return source is DataSubscriber subscriber ? subscriber.MinimumMeasurementsPerSecond : 0.0D;
    }

    private static double GetSubscriberStatistic_MaximumMeasurementsPerSecond(object source, string arguments)
    {
        return source is DataSubscriber subscriber ? subscriber.MaximumMeasurementsPerSecond : 0.0D;
    }

    private static double GetSubscriberStatistic_AverageMeasurementsPerSecond(object source, string arguments)
    {
        return source is DataSubscriber subscriber ? subscriber.AverageMeasurementsPerSecond : 0.0D;
    }

    private static double GetSubscriberStatistic_LifetimeBytesReceived(object source, string arguments)
    {
        return source is DataSubscriber subscriber ? subscriber.TotalBytesReceived : 0.0D;
    }

    private static double GetSubscriberStatistic_LifetimeMinimumLatency(object source, string arguments)
    {
        return source is DataSubscriber subscriber ? subscriber.LifetimeMinimumLatency : 0.0D;
    }

    private static double GetSubscriberStatistic_LifetimeMaximumLatency(object source, string arguments)
    {
        return source is DataSubscriber subscriber ? subscriber.LifetimeMaximumLatency : 0.0D;
    }

    private static double GetSubscriberStatistic_LifetimeAverageLatency(object source, string arguments)
    {
        return source is DataSubscriber subscriber ? subscriber.LifetimeAverageLatency : 0.0D;
    }

    private static double GetSubscriberStatistic_UpTime(object source, string arguments)
    {
        return source is DataSubscriber subscriber ? subscriber.RunTime : 0.0D;
    }

    private static double GetSubscriberStatistic_TLSSecuredChannel(object source, string arguments)
    {
        return source is DataSubscriber subscriber && subscriber.SecurityMode == SecurityMode.TLS ? 1.0D : 0.0D;
    }

    #endregion

    #region [ Publisher Statistics ]

    private static double GetPublisherStatistic_Connected(object source, string arguments)
    {
        return source is DataPublisher publisher && publisher.IsConnected ? 1.0D : 0.0D;
    }

    private static double GetPublisherStatistic_ConnectedClientCount(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.Count : 0.0D;
    }

    private static double GetPublisherStatistic_ProcessedMeasurements(object source, string arguments)
    {
        double statistic = source is DataPublisher publisher ? publisher.ProcessedMeasurements : 0.0D;
        return s_statisticValueCache.GetDifference(source, statistic, nameof(DataPublisher.ProcessedMeasurements));
    }

    private static double GetPublisherStatistic_TotalBytesSent(object source, string arguments)
    {
        double statistic = source is DataPublisher publisher ? publisher.TotalBytesSent : 0.0D;
        double difference = s_statisticValueCache.GetDifference(source, statistic, nameof(DataPublisher.TotalBytesSent));
        return difference < 0.0D ? statistic : difference;
    }

    private static double GetPublisherStatistic_LifetimeMeasurements(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.LifetimeMeasurements : 0.0D;
    }

    private static double GetPublisherStatistic_MinimumMeasurementsPerSecond(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.MinimumMeasurementsPerSecond : 0.0D;
    }

    private static double GetPublisherStatistic_MaximumMeasurementsPerSecond(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.MaximumMeasurementsPerSecond : 0.0D;
    }

    private static double GetPublisherStatistic_AverageMeasurementsPerSecond(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.AverageMeasurementsPerSecond : 0.0D;
    }

    private static double GetPublisherStatistic_LifetimeBytesSent(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.TotalBytesSent : 0.0D;
    }

    private static double GetPublisherStatistic_LifetimeMinimumLatency(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.LifetimeMinimumLatency : 0.0D;
    }

    private static double GetPublisherStatistic_LifetimeMaximumLatency(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.LifetimeMaximumLatency : 0.0D;
    }

    private static double GetPublisherStatistic_LifetimeAverageLatency(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.LifetimeAverageLatency : 0.0D;
    }

    private static double GetPublisherStatistic_BufferBlockRetransmissions(object source, string arguments)
    {
        double statistic = source is DataPublisher publisher ? publisher.BufferBlockRetransmissions : 0.0D;
        return s_statisticValueCache.GetDifference(source, statistic, nameof(DataPublisher.BufferBlockRetransmissions));
    }

    private static double GetPublisherStatistic_UpTime(object source, string arguments)
    {
        return source is DataPublisher publisher ? publisher.RunTime : 0.0D;
    }

    private static double GetPublisherStatistic_TLSSecuredChannel(object source, string arguments)
    {
        return source is DataPublisher publisher && publisher.SecurityMode == SecurityMode.TLS ? 1.0D : 0.0D;
    }

    #endregion

    private static readonly StatisticValueStateCache s_statisticValueCache = new();
}

#pragma warning restore IDE0060 // Remove unused parameter
