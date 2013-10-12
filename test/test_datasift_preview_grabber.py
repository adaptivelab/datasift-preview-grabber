"""
Copyright 2013 Adaptive Lab

This file is part of the datasift_preview_grabber script.

The datasift_preview_grabber script is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

The datasift_preview_grabber script is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with the datasift_preview_grabber script.  If not, see <http://www.gnu.org/licenses/>.
"""
import datasift
import datetime
from hamcrest import instance_of, match_equality
import json
import mock
from mock import call, ANY
import pytz
from os import path
from datasift_preview_grabber import (GetPreviewStatsTask,
                                      PreviewStatsCommand,
                                      TimespanSplitter,
                                      GetPreviewTaskManager)
from unittest import TestCase


def get_fixture_file(filename):
    """Helper to load the contents of a fixture file."""
    return open(path.join(path.abspath(path.dirname(__file__)),
                          "fixtures",
                          filename))

class GetPreviewStatsTaskTestCase(TestCase):

    def create_task(self, user, start=None, end=None, stream_hash=None):
        start = start or datetime.datetime(2013, 03, 01, 00).replace(
            tzinfo=pytz.utc)
        end = end or datetime.datetime(2013, 03, 01, 01).replace(
            tzinfo=pytz.utc)
        stream_hash = stream_hash or "sadgnjkwer"
        return GetPreviewStatsTask(user, start, end, stream_hash, timeout=0.1)

    def get_preview_api_responses(self):
        created_response = {
            u'created_at': 1364487833,
            u'id': u'd48077141e6cf6c71c01'
        }
        in_progress_response = json.loads(
            get_fixture_file("preview-get-in-progress-response.json").read())
        complete_response = json.loads(get_fixture_file(
                "example-historics-preview-api-response.json").read())
        return [created_response, in_progress_response, complete_response]

    def test_get_stats_correctly_calls_create_preview(self):
        mock_user = mock.Mock(spec=datasift.User)
        mock_user.call_api.return_value = {
            u'created_at': 1364487833,
            u'id': u'd48077141e6cf6c71c01'
        }
        start = datetime.datetime(2013, 03, 01, 00).replace(
            tzinfo=pytz.utc)
        end = datetime.datetime(2013, 03, 01, 01).replace(
            tzinfo=pytz.utc)
        stream_hash = "asdfasdf"
        get_stats_task = self.create_task(mock_user, start, end, stream_hash)
        get_stats_task.create()
        mock_user.call_api.assert_called_with(
            "preview/create",
            {
                "start": 1362096000,
                "end": 1362099600,
                "hash": "asdfasdf",
                "parameters": "interaction.id,targetVol,hour",
            }
        )

    def test_get_stats_called_before_create_raises(self):
        mock_user = mock.Mock(spec=datasift.User)
        stats_task = self.create_task(mock_user)
        with self.assertRaises(RuntimeError):
            stats_task.get_result()

    def test_get_result_calls_get_until_response_returned(self):
        mock_user = mock.Mock(datasift.User)
        api_responses = self.get_preview_api_responses()
        mock_user.call_api.side_effect = api_responses
        stream_hash = "asdfasdf"
        get_stats_task = self.create_task(mock_user, stream_hash=stream_hash)
        get_stats_task.create()
        result = get_stats_task.get_result()
        # The final result returned is the complete preview response
        self.assertEqual(result, api_responses[-1])

        expected_params = {"id": 'd48077141e6cf6c71c01'}
        expected_calls = [call("preview/get", expected_params)] * 2
        self.assertEqual(expected_calls, mock_user.call_api.call_args_list[1:])


@mock.patch("datasift_preview_grabber.GetPreviewTaskManager",
            spec=GetPreviewTaskManager)
class GetPreviewStatsCommandTestCase(TestCase):

    def test_date_args_parsed_correctly(self, mock_preview_task_cons):
        start = datetime.datetime(2013, 03, 03).replace(tzinfo=pytz.utc)
        end = datetime.datetime(2013, 04, 03).replace(tzinfo=pytz.utc)
        with mock.patch("datasift_preview_grabber.TimespanSplitter") as \
                mock_timespan_cons:
            PreviewStatsCommand(
                argv=["2013-03-03", "2013-04-03", "somehash", "dsusername",
                      "dsapikey"])
            mock_timespan_cons.assert_called_with(start, end)

    def test_task_manager_instantiated_correctly(self, mock_taskmanager_cons):
        PreviewStatsCommand(
            argv=["2013-03-03", "2013-04-03", "somehash", "dsusername",
                  "dsapikey"])
        timespan_type_matcher = match_equality(instance_of(TimespanSplitter))
        mock_taskmanager_cons.assert_called_with(
            ANY,
            timespan_type_matcher,
            "somehash")

    def test_run_returns_the_results(self, mock_taskmanager_cons):
        mock_taskmanager_cons.return_value.get_results.return_value = \
                "response"
        command = PreviewStatsCommand(
            argv=["2013-03-03", "2013-04-03", "somehash", "dsusername",
                  "dsapikey"])
        self.assertEqual("response", command.run())

    def test_command_raises_if_start_date_after_end_date(self, mock_taskmanager_cons):
        with self.assertRaises(ValueError):
            PreviewStatsCommand(
                argv=["2013-03-03", "2013-02-03", "somehash", "dsusername",
                      "dsapikey"])


class TimespanSplitterTestCase(TestCase):

    def test_timespans_split_into_twenty_four_hour_segments(self):
        start_date = datetime.datetime(2013, 01, 01).replace(tzinfo=pytz.utc)
        end_date = datetime.datetime(2013, 01, 05).replace(tzinfo=pytz.utc)
        splitter = TimespanSplitter(start_date, end_date)
        splits = splitter.get_splits()
        self.assertEqual(len(splits), 4)
        one_day = datetime.timedelta(days=1)
        self.assertEqual(splits[0], (start_date, start_date + one_day))
        self.assertEqual(splits[1], (start_date + one_day, start_date +
                                     2*one_day))
        self.assertEqual(splits[-1], (end_date - one_day, end_date))

    def test_timespans_over_non_day_intervals_split_correctly(self):
        start = datetime.datetime(2013, 01, 01).replace(tzinfo=pytz.utc)
        end = datetime.datetime(2013, 01, 02, 05).replace(tzinfo=pytz.utc)
        splitter = TimespanSplitter(start, end)
        splits = splitter.get_splits()
        self.assertEqual(len(splits), 2)
        one_day = datetime.timedelta(days=1)
        self.assertEqual(splits[0], (start, start + one_day))
        self.assertEqual(splits[1], (start + one_day,
                                     start + datetime.timedelta(days=1,
                                                                hours=5)))

    def test_sub_day_timespans_split_correctly(self):
        start = datetime.datetime(2013, 01, 01).replace(tzinfo=pytz.utc)
        end = datetime.datetime(2013, 01, 01, 05).replace(tzinfo=pytz.utc)
        splitter = TimespanSplitter(start, end)
        splits = splitter.get_splits()
        self.assertEqual(len(splits), 1)
        self.assertEqual(splits[0],
                         (start, start + datetime.timedelta(hours=5)))


class GetPreviewTaskManagerTestCase(TestCase):

    def create_mock_preview_task(self):
        mock_task = mock.Mock(spec=GetPreviewStatsTask)
        complete_response = json.loads(get_fixture_file(
                "example-historics-preview-api-response.json").read())
        mock_task.get_result.return_value = complete_response
        return mock_task

    @mock.patch("datasift_preview_grabber.GetPreviewStatsTask",
            spec=GetPreviewStatsTask)
    def test_multiple_tasks_output_concatenated(self, mock_preview_cons):
        mock_user = mock.Mock(spec=datasift.User)
        mock_splitter = mock.Mock(spec=TimespanSplitter)
        splits = [
            (datetime.datetime(2013, 01, 01, 00),
             datetime.datetime(2013, 01, 02, 00)),
            (datetime.datetime(2013, 01, 02, 00),
             datetime.datetime(2013, 01, 03, 00)),
        ]
        mock_splitter.get_splits.return_value = splits
        stream_hash = "somehash"
        mock_preview_tasks = [self.create_mock_preview_task(),
                              self.create_mock_preview_task()]
        mock_preview_cons.side_effect = mock_preview_tasks
        construction_calls = [call(ANY, splits[0][0], splits[0][1], "somehash"),
                              call(ANY, splits[1][0], splits[1][1], "somehash")]
        task_manager = GetPreviewTaskManager(mock_user, mock_splitter, stream_hash)
        result = task_manager.get_results()
        self.assertEqual(construction_calls, mock_preview_cons.call_args_list)
        for mock_task in mock_preview_tasks:
            mock_task.create.assert_called_with()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["hash"], "2367f6dbb8a59ec311b36133d817bd9a")
