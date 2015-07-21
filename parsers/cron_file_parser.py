#!/usr/bin/env python
"""Simple parsers for cron type files."""


import crontab
import winjob

from grr.lib import parsers
from grr.lib import utils
from grr.lib.rdfvalues import cronjobs as rdf_cronjobs


class CronTabParser(parsers.FileParser):
  """Parser for crontab files."""

  output_types = ["CronTabFile"]
  supported_artifacts = ["OSXCronTabs", "LinuxCronTabs"]

  def Parse(self, stat, file_object, knowledge_base):
    """Parse the crontab file."""
    _ = knowledge_base
    entries = []

    cron_data = file_object.read(100000)
    jobs = crontab.CronTab(tab=cron_data)

    for job in jobs:
      entries.append(
          rdf_cronjobs.CronTabEntry(minute=utils.SmartStr(job.minute),
                                    hour=utils.SmartStr(job.hour),
                                    dayofmonth=utils.SmartStr(job.dom),
                                    month=utils.SmartStr(job.month),
                                    dayofweek=utils.SmartStr(job.dow),
                                    command=utils.SmartStr(job.command),
                                    comment=utils.SmartStr(job.comment)))

    yield rdf_cronjobs.CronTabFile(aff4path=stat.aff4path, jobs=entries)

class WinTaskParser(parsers.FileParser):
  """Parser for Windows task files."""

  output_types = ["WinTask"]
  supported_artifacts = ["ScheduledTasks"]

  def Parse(self, stat, file_object, knowledge_base):
    """Parse the task file."""
    _ = knowledge_base

    task_data = file_object.read(100000)
    try:
      task_obj = winjob.read_task(task_data)
      task_dict = task_obj.parse()
      action_list = []
      for action in task_dict.get("actions"):
        task_action = rdf_cronjobs.WinTaskAction(
          action_type = action.get("action_type"),
          exec_command = action.get("exec_command"),
          exec_arguments = action.get("exec_arguments"),
          exec_working_directory = action.get("exec_working_directory"),
          comhandler_classid = action.get("comhandler_classid"),
          comhandler_data = action.get("comhandler_data"),
          sendemail_server = action.get("sendemail_server"),
          sendemail_subject = action.get("sendemail_subject"),
          sendemail_to = action.get("sendemail_to"),
          sendemail_cc = action.get("sendemail_cc"),
          sendemail_bcc = action.get("sendemail_bcc"),
          sendemail_replyto = action.get("sendemail_replyto"),
          sendemail_from = action.get("sendemail_from"),
          sendemail_body = action.get("sendemail_body"),
          sendemail_attachements = action.get("sendemail_attachements"),
          sendemail_headers = action.get("sendemail_headers"),
          showmessage_title = action.get("showmessage_title"),
          showmessage_body = action.get("showmessage_body"))
        action_list.append(task_action)
      task = rdf_cronjobs.WinTask(aff4path=stat.aff4path,
                                  status = task_dict.get("status"),
                                  priority = task_dict.get("priority"),
                                  name = task_dict.get("name"),
                                  application_name = task_dict.get("application_name"),
                                  parameters = task_dict.get("parameters"),
                                  comment = task_dict.get("comment"),
                                  working_directory = task_dict.get("working_directory"),
                                  account_name = task_dict.get("account_name"),
                                  account_run_level = task_dict.get("account_run_level"),
                                  account_logon_type = task_dict.get("account_logon_type"),
                                  creator = task_dict.get("creator"),
                                  exit_code = task_dict.get("exit_code"),
                                  creation_date = task_dict.get("creation_date"),
                                  most_recent_run_time = task_dict.get("most_recent_run_time"),
                                  max_run_time = task_dict.get("max_run_time"),
                                  next_run_time = task_dict.get("next_run_time"),
                                  action_list = action_list)
    except winjob.FormatError:
      task = rdf_cronjobs.WinTask(aff4path=stat.aff4path,
                                  name="Unable to parse task file.")
    yield task