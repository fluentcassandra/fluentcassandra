﻿using System;
using System.Linq;
using Xunit;

namespace FluentCassandra
{

    public class GuidGeneratorTest
    {
        [Fact]
        public void Type1Check()
        {
            // arrange
            var expected = GuidVersion.TimeBased;
            var guid = GuidGenerator.GenerateTimeBasedGuid();

            // act
            var actual = guid.GetVersion();

            // assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void SanityType1Check()
        {
            // arrange
            var expected = GuidVersion.TimeBased;
            var guid = Guid.NewGuid();

            // act
            var actual = guid.GetVersion();

            // assert
            Assert.NotEqual(expected, actual);
        }

        [Fact]
        public void GetDateTimeUnspecified()
        {
            // arrange
            var expected = new DateTime(1980, 3, 14, 12, 23, 42, 112, DateTimeKind.Unspecified);
            var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

            // act
            var actual = GuidGenerator.GetDateTime(guid).ToLocalTime();

            // assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetDateTimeLocal()
        {
            // arrange
            var expected = new DateTime(1980, 3, 14, 12, 23, 42, 112, DateTimeKind.Local);
            var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

            // act
            var actual = GuidGenerator.GetLocalDateTime(guid);

            // assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetDateTimeUtc()
        {
            // arrange
            var expected = new DateTime(1980, 3, 14, 12, 23, 42, 112, DateTimeKind.Utc);
            var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

            // act
            var actual = GuidGenerator.GetUtcDateTime(guid);

            // assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetDateTimeOffset()
        {
            // arrange
            var expected = new DateTimeOffset(1980, 3, 14, 12, 23, 42, 112, TimeSpan.Zero);
            var guid = GuidGenerator.GenerateTimeBasedGuid(expected);

            // act
            var actual = GuidGenerator.GetDateTimeOffset(guid);

            // assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void DoesNotCreateDuplicateWhenTimeHasNotPassed()
        {
            // arrange
            DateTimeOffset currentTime = DateTimeOffset.UtcNow;

            TimeUUIDHelper.UtcNow = () => currentTime;  //make sure all calls will return the same time
            Guid firstGuid = GuidGenerator.GenerateTimeBasedGuid();

            // act
            Guid secondGuid = GuidGenerator.GenerateTimeBasedGuid();

            // assert
            Assert.True(firstGuid != secondGuid, "first: " + firstGuid + " second: " + secondGuid);
            //Assert.NotEqual(firstGuid,secondGuid);
        }

        [Fact]
        public void ClockSequenceChangesWhenTimeMovesBackward()
        {
            // arrange
            DateTimeOffset currentTime = DateTimeOffset.UtcNow;

            TimeUUIDHelper.UtcNow = () => currentTime;
            Guid firstGuid = GuidGenerator.GenerateTimeBasedGuid();

            // act
            TimeUUIDHelper.UtcNow = () => currentTime.AddTicks(-1);  //make sure clock went backwards
            Guid secondGuid = GuidGenerator.GenerateTimeBasedGuid();

            // assert
            //clock sequence is not equal
            Assert.NotEqual(GuidGenerator.GetClockSequence(firstGuid), GuidGenerator.GetClockSequence(secondGuid));
        }
    }
}