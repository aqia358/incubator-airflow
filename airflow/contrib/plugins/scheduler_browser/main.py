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

        name = "xxxx"
        return self.render(
            "scheduler_browser/dbs.html", name=name)

    @expose('/table/')
    def table(self):
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=1)
        sql = """
            select 
                dag_id, 
                state,
                min(start_date) as start_date, 
                max(end_date) as  end_date, 
                max(end_date)-min(start_date) as  duration
            from task_instance 
            where 
                `start_date` >= "{start_date}" 
                and  `start_date` < "{end_date}" 
                and state != 'failed'
            group by dag_id 
            order by start_date;
        """.format(start_date=start_date, end_date=end_date)
        h = MySqlHook(METASTORE_MYSQL_CONN_ID)
        rows = h.get_records(sql)
        tasks = []
        taskNames = []
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
            tasks.append(task)

        data = {
            'height': 20 * len(taskNames),
            'tasks': tasks,
            'taskNames': taskNames,
            'taskStatus': {'success':'success'}
        }

        # data = {'height': 175, 'tasks': [{'status': 'success', 'startDate': 1519926448000, 'endDate': 1519926449000,
        #                                   'isoEnd': '2018-03-02T01:47:29.13', 'taskName': 'runme_0',
        #                                   'executionDate': '2018-03-01T00:00:00', 'isoStart': '2018-03-02T01:47:28.06',
        #                                   'duration': '0:00:01.07'},
        #                                  {'status': 'success', 'startDate': 1519926454000, 'endDate': 1519926455000,
        #                                   'isoEnd': '2018-03-02T01:47:35.27', 'taskName': 'runme_2',
        #                                   'executionDate': '2018-03-01T00:00:00', 'isoStart': '2018-03-02T01:47:34.21',
        #                                   'duration': '0:00:01.05'},
        #                                  {'status': 'success', 'startDate': 1519926460000, 'endDate': 1519926461000,
        #                                   'isoEnd': '2018-03-02T01:47:41.48', 'taskName': 'runme_1',
        #                                   'executionDate': '2018-03-01T00:00:00', 'isoStart': '2018-03-02T01:47:40.34',
        #                                   'duration': '0:00:01.13'},
        #                                  {'status': 'success', 'startDate': 1519926466000, 'endDate': 1519926467000,
        #                                   'isoEnd': '2018-03-02T01:47:47.70', 'taskName': 'also_run_this',
        #                                   'executionDate': '2018-03-01T00:00:00', 'isoStart': '2018-03-02T01:47:46.59',
        #                                   'duration': '0:00:01.10'},
        #                                  {'status': 'success', 'startDate': 1519926585000, 'endDate': 1519926586000,
        #                                   'isoEnd': '2018-03-02T01:49:46.65', 'taskName': 'run_after_loop',
        #                                   'executionDate': '2018-03-01T00:00:00', 'isoStart': '2018-03-02T01:49:45.61',
        #                                   'duration': '0:00:01.03'},
        #                                  {'status': 'success', 'startDate': 1519926649000, 'endDate': 1519926650000,
        #                                   'isoEnd': '2018-03-02T01:50:50.04', 'taskName': 'run_this_last',
        #                                   'executionDate': '2018-03-01T00:00:00', 'isoStart': '2018-03-02T01:50:49.00',
        #                                   'duration': '0:00:01.04'}],
        #         'taskNames': ['runme_1', 'runme_2', 'runme_0', 'also_run_this', 'run_after_loop', 'run_this_last'],
        #         'taskStatus': {'success': 'success'}}
        return self.render(
            "scheduler_browser/gantt.html",
            data=data)

    @expose('/db/')
    def db(self):
        db = request.args.get("db")
        m = HiveMetastoreHook(METASTORE_CONN_ID)
        tables = sorted(m.get_tables(db=db), key=lambda x: x.tableName)
        return self.render(
            "scheduler_browser/db.html", tables=tables, db=db)

    @wwwutils.gzipped
    @expose('/partitions/')
    def partitions(self):
        schema, table = request.args.get("table").split('.')
        sql = """
        SELECT
            a.PART_NAME,
            a.CREATE_TIME,
            c.LOCATION,
            c.IS_COMPRESSED,
            c.INPUT_FORMAT,
            c.OUTPUT_FORMAT
        FROM PARTITIONS a
        JOIN TBLS b ON a.TBL_ID = b.TBL_ID
        JOIN DBS d ON b.DB_ID = d.DB_ID
        JOIN SDS c ON a.SD_ID = c.SD_ID
        WHERE
            b.TBL_NAME like '{table}' AND
            d.NAME like '{schema}'
        ORDER BY PART_NAME DESC
        """.format(**locals())
        h = MySqlHook(METASTORE_MYSQL_CONN_ID)
        df = h.get_pandas_df(sql)
        return df.to_html(
            classes="table table-striped table-bordered table-hover",
            index=False,
            na_rep='',)

    @wwwutils.gzipped
    @expose('/objects/')
    def objects(self):
        where_clause = ''
        if DB_WHITELIST:
            dbs = ",".join(["'" + db + "'" for db in DB_WHITELIST])
            where_clause = "AND b.name IN ({})".format(dbs)
        if DB_BLACKLIST:
            dbs = ",".join(["'" + db + "'" for db in DB_BLACKLIST])
            where_clause = "AND b.name NOT IN ({})".format(dbs)
        sql = """
        SELECT CONCAT(b.NAME, '.', a.TBL_NAME), TBL_TYPE
        FROM TBLS a
        JOIN DBS b ON a.DB_ID = b.DB_ID
        WHERE
            a.TBL_NAME NOT LIKE '%tmp%' AND
            a.TBL_NAME NOT LIKE '%temp%' AND
            b.NAME NOT LIKE '%tmp%' AND
            b.NAME NOT LIKE '%temp%'
        {where_clause}
        LIMIT {LIMIT};
        """.format(where_clause=where_clause, LIMIT=TABLE_SELECTOR_LIMIT)
        h = MySqlHook(METASTORE_MYSQL_CONN_ID)
        d = [
                {'id': row[0], 'text': row[0]}
            for row in h.get_records(sql)]
        return json.dumps(d)

    @wwwutils.gzipped
    @expose('/data/')
    def data(self):
        table = request.args.get("table")
        sql = "SELECT * FROM {table} LIMIT 1000;".format(table=table)
        h = PrestoHook(PRESTO_CONN_ID)
        df = h.get_pandas_df(sql)
        return df.to_html(
            classes="table table-striped table-bordered table-hover",
            index=False,
            na_rep='',)

    @expose('/ddl/')
    def ddl(self):
        table = request.args.get("table")
        sql = "SHOW CREATE TABLE {table};".format(table=table)
        h = HiveCliHook(HIVE_CLI_CONN_ID)
        return h.run_cli(sql)

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
