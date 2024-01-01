/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.utils;

import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.TimeZone;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.SQL_LOCAL_DATE_TIME;
import static io.pixelsdb.pixels.core.utils.DatetimeUtils.stringTimeToMillis;

/**
 * @author hank
 * @create 2023-04-18
 */
public class TestDateTimeUtils
{
    @Test
    public void testTimeFormat()
    {
        Time time = new Time(stringTimeToMillis("15:36:59.123"));
        System.out.println(time);
        System.out.println(time.getTime());
        long timestamp = LocalDateTime.of(
                1970, 1, 1, 23, 59, 59,
                128000000).toEpochSecond(ZoneOffset.UTC);
        System.out.println(timestamp);
        System.out.println(LocalTime.parse("20:38:47.123").toNanoOfDay());
        LocalDateTime localDateTime = LocalDateTime.parse("1970-01-01 20:38:47.12345", SQL_LOCAL_DATE_TIME);
        System.out.println(localDateTime.toEpochSecond(ZoneOffset.UTC));
        System.out.println(localDateTime.getNano());
    }

    @Test
    public void testGetDaysSinceEpoch()
    {
        int day1, day2, day3;
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringDateToDay("2023-03-05");
        day3 = DatetimeUtils.localMillisToUtcDays(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringDateToDay("2023-03-05");
        day3 = DatetimeUtils.localMillisToUtcDays(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringDateToDay("2023-03-05");
        day3 = DatetimeUtils.localMillisToUtcDays(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+12:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringDateToDay("2023-03-05");
        day3 = DatetimeUtils.localMillisToUtcDays(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+14:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringDateToDay("2023-03-05");
        day3 = DatetimeUtils.localMillisToUtcDays(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT-8:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringDateToDay("2023-03-05");
        day3 = DatetimeUtils.localMillisToUtcDays(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT-12:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringDateToDay("2023-03-05");
        day3 = DatetimeUtils.localMillisToUtcDays(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT-14:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringDateToDay("2023-03-05");
        day3 = DatetimeUtils.localMillisToUtcDays(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;
    }
}
