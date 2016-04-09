#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Convert spark event log JSON to CSV
# @ Jakob Frick

from sets import Set
import argparse
import json


class SparkEvent:

    prefix = "SparkListener"

    def __init__(self, data):
        self.event = data["Event"][len(SparkEvent.prefix):]
        self.raw = "\"" + str(json.dumps(data)) + "\""


class Stage(SparkEvent):

    stages = {}

    def __init__(self, data):
        SparkEvent.__init__(self, data)
        stage_info = data["Stage Info"]

        self.id = stage_info["Stage ID"]
        self.attempt = stage_info["Stage Attempt ID"]
        self.name = stage_info["Stage Name"]
        self.number_tasks = stage_info["Number of Tasks"]
        self.rdd_info = "\"" + str(stage_info["RDD Info"]) + "\""

    def save(self):
        Stage.stages[self.id] = self

    def as_csv(self):
        values = [self.event,
                  self.id,
                  self.attempt,
                  self.name,
                  self.number_tasks,
                  self.rdd_info,
                  self.raw]

        return ";".join(map(lambda x: str(x), values))

    @staticmethod
    def update(data):
        Stage.stages[int(data["Stage Info"]["Stage ID"])] = Stage(data)
        return int(data["Stage Info"]["Stage ID"])

    @staticmethod
    def headers():
        values = ["Event",
                  "ID",
                  "Attempt ID",
                  "Name",
                  "Number of Tasks",
                  "RDD Info",
                  "Raw"]
        return ";".join(values)


class Job(SparkEvent):

    jobs = {}

    def __init__(self, data):
        SparkEvent.__init__(self, data)
        self.id = int(data["Job ID"])
        self.submission_time = data["Submission Time"]
        self.stages_info = ",".join(
                                map(lambda stage:
                                    str((stage['Stage ID'],
                                         stage['Stage Attempt ID'])),
                                    data["Stage Infos"]))

    def end(self, data):
        self.completion_time = data["Completion Time"]
        self.result = data["Job Result"]["Result"]
        self.save()

    def save(self):
        Job.jobs[self.id] = self

    def as_csv(self):
        values = [self.event,
                  self.id,
                  self.stages_info,
                  self.submission_time,
                  self.completion_time,
                  self.result,
                  self.raw]

        return ";".join(map(lambda x: str(x), values))

    @staticmethod
    def update(data):
        Job.jobs[int(data["Job ID"])].end(data)
        return int(data["Job ID"])

    @staticmethod
    def headers():
        values = ["Event",
                  "ID",
                  "Stages Info",
                  "Submission time",
                  "Completion time",
                  "Result",
                  "Raw"]
        return ";".join(values)


class Task(SparkEvent):

    tasks = {}

    def __init__(self, data):
        SparkEvent.__init__(self, data)

        self.stage_id = data["Stage ID"]
        self.stage_attempt = data["Stage Attempt ID"]
        task_info = data["Task Info"]

        self.id = int(task_info["Task ID"])
        self.index = task_info["Index"]
        self.attempt = task_info["Attempt"]
        self.launch_time = task_info["Launch Time"]
        self.executor_id = task_info["Executor ID"]
        self.host = task_info["Host"]
        self.getting_result_time = task_info["Getting Result Time"]
        self.finish_time = task_info["Finish Time"]
        self.failed = task_info["Failed"]

    def save(self):
        Task.tasks[self.id] = self

    def finish(self, data):
        self.__init__(data)
        self.type = data["Task Type"]
        self.end_reason = data["Task End Reason"]["Reason"]
        self.save()

    def as_csv(self):
        values = [self.event,
                  self.id,
                  self.index,
                  self.attempt,
                  self.executor_id,
                  self.host,
                  self.launch_time,
                  self.getting_result_time,
                  self.finish_time,
                  self.failed,
                  self.raw]
        return ";".join(map(lambda x: str(x), values))

    @staticmethod
    def update(data):
        Task.tasks[int(data["Task Info"]["Task ID"])].finish(data)
        return int(data["Task Info"]["Task ID"])

    @staticmethod
    def headers():
        values = ["Event",
                  "ID",
                  "Index",
                  "Attempt",
                  "Executor ID",
                  "Host",
                  "Launch time",
                  "Getting result time",
                  "Finish time",
                  "Failed",
                  "Raw"]
        return ";".join(values)


def get_csv_from_json(event):

    event_type = event["Event"][len(SparkEvent.prefix):]
    values = []

    if event_type == "LogStart":
        values += ["Spark Version: " + event["Spark Version"]]

    if event_type == "ExecutorAdded":
        values += [event["Executor ID"], json.dumps(event["Executor Info"]),
                   "Timestamp: " + str(event["Timestamp"])]

    if event_type == "BlockManagerAdded":
        values += [json.dumps(event["Block Manager ID"]),
                   "Timestamp: " + str(event["Timestamp"])]

    if event_type == "EnvironmentUpdate":
        values += [json.dumps(event["JVM Information"])]

    if event_type == "ApplicationStart":
        values += [event["App Name"], event["App ID"], event["User"],
                   "Timestamp: " + str(event["Timestamp"])]

    if event_type == "UnpersistRDD":
        values += [str(event["RDD ID"])]

    if event_type == "ApplicationEnd":
        values += ["Timestamp: " + str(event["Timestamp"])]

    if event_type == "TaskStart":
        task = Task(event)
        task.save()
        values += [str(task.id)]

    if event_type == "TaskEnd":
        task_id = Task.update(event)
        values += [str(task_id)]

    if event_type == "JobStart":
        job = Job(event)
        job.save()
        values += [str(job.id)]

    if event_type == "JobEnd":
        job_id = Job.update(event)
        values += [str(job_id)]

    if event_type == "StageSubmitted":
        stage = Stage(event)
        stage.save()
        values += [str(stage.id)]

    if event_type == "StageCompleted":
        stage_id = Stage.update(event)
        values += [str(stage_id)]

    return event_type + ";" + ";".join(values)


def write_dict(filename, event_type, events):
    with open(filename, "w") as file:
        file.write(event_type.headers() + "\n")

        for key in events:
            file.write(events[key].as_csv() + "\n")
        file.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="the filename of the Spark eventlog",
                        type=str)
    args = parser.parse_args()

    if args.filename.rfind(".json") != len(args.filename) - len(".json"):
        outName = args.filename
    else:
        outName = args.filename[:len(args.filename) - len(".json")]

    outfile = open(outName + ".csv", "w")

    with open(args.filename) as file:
        for line in file:
            event = json.loads(line)
            csvLine = get_csv_from_json(event)
            outfile.write(csvLine + "\n")
        outfile.close()

        write_dict(outName + "_stages_" + ".csv", Stage, Stage.stages)
        write_dict(outName + "_jobs_" + ".csv", Job, Job.jobs)
        write_dict(outName + "_tasks_" + ".csv", Task, Task.tasks)

if __name__ == "__main__":
    main()
