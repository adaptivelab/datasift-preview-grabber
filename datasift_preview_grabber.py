"""
Copyright 2013 Adaptive Lab

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
import calendar
import datasift
from dateutil.parser import parse
from docopt import docopt
import datetime
import logging
import pytz
import time
import json

logging.basicConfig(filename='datasift_preview_grabber.log', level=logging.INFO)
logger = logging.getLogger(__name__)


class PreviewStatsCommand(object):

    def __init__(self, argv=None):
        usage = """Usage: datasift_preview_grabber <start_date> <end_date> <stream_hash> <datasift-username> <datasift-apikey>"""
        options = docopt(usage, argv=argv)
        start = parse(options["<start_date>"]).replace(tzinfo=pytz.utc)
        end = parse(options["<end_date>"]).replace(tzinfo=pytz.utc)
        if (end - start).total_seconds() <= 0:
            raise ValueError("Start date must be before end date, "
                             "start was {0}, end date was {1}".format(
                                 options["<start_date>"],
                                 options["<end_date>"]))

        user = datasift.User(options["<datasift-username>"], options["<datasift-apikey>"])
        stream_hash = options["<stream_hash>"]
        splitter = TimespanSplitter(start, end)
        self.get_task_manager = GetPreviewTaskManager(user, splitter, stream_hash)

    def run(self):
        """Returns the combined results of the individual preview grabs for each
        day in the specified date range."""
        return self.get_task_manager.get_results()


class GetPreviewStatsTask(object):

    def __init__(self, datasift_user, start, end, stream_hash, timeout=30):
        self.datasift_user = datasift_user
        self.start = start
        self.end = end
        self.stream_hash = stream_hash
        self.timeout = timeout
        self._id = None

    def create(self):
        parameters = {
            "start": calendar.timegm(self.start.timetuple()),
            "end": calendar.timegm(self.end.timetuple()),
            "parameters": "interaction.id,targetVol,hour",
            "hash": self.stream_hash,
        }
        logger.info("Creating preview, parameters are {0}".format(
            parameters))
        response = self.datasift_user.call_api("preview/create", parameters)
        logger.info("Rate limit: {1} remaining (of {0})".format(
                    self.datasift_user.get_rate_limit(),
                    self.datasift_user.get_rate_limit_remaining()))
        logger.info("Create preview response was {0}".format(response))
        self._id = response["id"]

    def get_result(self):
        if self._id is None:
            raise RuntimeError("You must call get_stats_task.create before get_result")
        parameters = {
            "id": self._id
        }
        while True:
            response = self.datasift_user.call_api("preview/get", parameters)
            logger.info("Rate limit: {1} remaining (of {0})".format(
                    self.datasift_user.get_rate_limit(),
                    self.datasift_user.get_rate_limit_remaining()))
            if response["status"] in ["queued", "prep", "submitted"]:
                logger.info("Preview task status: {0}".format(response["status"]))
            elif response["status"] == "running":
                logger.info("Preview task running, {0}% complete".format(
                    response["progress"]))
            elif response["status"] == "succeeded":
                return response
            else:
                raise RuntimeError("Unknown preview task status returned from datasift "
                                   "full response was {0}".format(response))
            logger.info("Waiting {0} seconds before retrying".format(
                self.timeout))
            time.sleep(self.timeout)


class TimespanSplitter(object):

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def get_splits(self):
        splits = []
        one_day = datetime.timedelta(days=1)
        next_split = self.start
        while next_split < self.end:
            delta = self.end - next_split
            if delta > one_day:
                splits.append((next_split, next_split + one_day))
            else:
                splits.append((next_split, next_split + delta))
            next_split += one_day
        return splits


class GetPreviewTaskManager(object):

    def __init__(self, user, timespan_splitter, stream_hash):
        self.user = user
        self.splitter = timespan_splitter
        self.stream_hash = stream_hash

    def get_results(self):
        result = []
        splits = self.splitter.get_splits()
        for start_date, end_date in splits:
            logger.debug("Getting preview data for {0} to {1}".format(
                start_date, end_date))
            task = GetPreviewStatsTask(self.user, start_date, end_date, self.stream_hash)
            task.create()
            result.append(task.get_result())
        return result


def main():
    """Entry point for running this script from the command line.  Prints the
    results to stdout."""
    command = PreviewStatsCommand()
    logger.info("Running the preview grab...")
    preview_results = command.run()
    logger.info("Dumping the results to stdout...")
    results_json = json.dumps(preview_results)
    print results_json
