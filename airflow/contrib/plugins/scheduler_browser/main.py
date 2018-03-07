# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta
import json
import time

from flask import Blueprint, request
from flask_admin import BaseView, expose
import pandas as pd

from airflow.hooks.hive_hooks import HiveMetastoreHook, HiveCliHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.presto_hook import PrestoHook
from airflow.plugins_manager import AirflowPlugin
from airflow.www import utils as wwwutils

METASTORE_CONN_ID = 'airflow_db'
METASTORE_MYSQL_CONN_ID = 'airflow_db'
PRESTO_CONN_ID = 'presto_default'
HIVE_CLI_CONN_ID = 'hive_default'
DEFAULT_DB = 'default'
DB_WHITELIST = None
DB_BLACKLIST = ['tmp']
TABLE_SELECTOR_LIMIT = 2000

# Keeping pandas from truncating long strings
pd.set_option('display.max_colwidth', -1)


# Creating a flask admin BaseView
class SchedulerBrowserView(BaseView, wwwutils.DataProfilingMixin):

    @expose('/')
    def index(self):
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=1)
        sql = """
            select 
                a.dag_id, 
                a.state, 
                min(a.start_date) as start_date, 
                max(a.end_date) as  end_date, 
                max(a.end_date)-min(a.start_date) as duration,
                b.job_type, 
                a.job_id
            from task_instance as a
            join job as b 
            ON a.job_id = b.id
            where 
                a.start_date >= "{start_date}" 
                and  a.start_date < "{end_date}" 
                and a.state != 'failed'
            group by a.dag_id, a.job_id 
            order by start_date;
        """.format(start_date=start_date, end_date=end_date)
        h = MySqlHook(METASTORE_MYSQL_CONN_ID)
        rows = h.get_records(sql)
        tasks = []
        taskNames = []
        name_set = set("")
        time_format = "%Y-%m-%dT%H:%M:%S"
        for row in rows:
            dag_id = row[0]
            state = row[1]
            start_date = row[2]
            end_date = row[3]
            duration = str(row[4])
            task = {'status': state,
                    'taskName': dag_id,
                    'startDate': time.mktime(start_date.timetuple()) * 1000,
                    'endDate': time.mktime(end_date.timetuple()) * 1000,
                    'executionDate': start_date.strftime(time_format),
                    'isoStart': start_date.strftime(time_format),
                    'isoEnd': end_date.strftime(time_format),
                    'duration': duration
                    }
            taskNames.append(dag_id)
            name_set.add(dag_id)
            tasks.append(task)

        data = {
            'height': 20 * len(name_set),
            'tasks': tasks,
            'taskNames': taskNames,
            'taskStatus': {'success':'success'}
        }

        return self.render(
            "scheduler_browser/gantt.html",
            data=data)

v = SchedulerBrowserView(category="Plugins", name="Scheduler Browser")

# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "scheduler_browser", __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/scheduler_browser')


# Defining the plugin class
class SchedulerBrowserView(AirflowPlugin):
    name = "scheduler_browser"
    flask_blueprints = [bp]
    admin_views = [v]
