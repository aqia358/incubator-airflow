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
from functools import wraps

import airflow
from airflow.models import User
from airflow.utils.db import provide_session
from flask import Blueprint, request, flash, redirect, url_for
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

def get_current_user(raw=True):
    try:
        if raw:
            res = airflow.login.current_user.user
        else:
            res = airflow.login.current_user
    except Exception as e:
        res = None
    return res


def admin_required(f):
    """Decorator for views requiring superuser access"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        current_user = get_current_user()
        if current_user and current_user.is_superuser():
            return f(*args, **kwargs)
        else:
            flash("This page requires superuser privileges", "error")
            return redirect(url_for('admin.index'))

    return decorated_function


# Creating a flask admin BaseView
class GroupManagerView(wwwutils.SuperUserMixin, BaseView):

    @expose('/')
    @provide_session
    def index(self, session=None):
        current_user = get_current_user()
        users = session.query(User).all()
        return self.render(
            "group_manager/group.html",
            data=users)


    @wwwutils.gzipped
    @expose('/superuser')
    @provide_session
    def superuser(self, session=None):
        user_id = request.args.get('user_id')
        is_superuser = request.args.get('is_superuser')
        user = session.query(User).filter(User.id == user_id).first()
        user.superuser = True if is_superuser == 'True' else False
        session.commit()
        return ''


    @wwwutils.gzipped
    @expose('/group')
    @provide_session
    def group(self, session=None):
        user_id = request.args.get('user_id')
        group = request.args.get('group')
        user = session.query(User).filter(User.id == user_id).first()
        user.group_name = group
        session.commit()
        return ''

v = GroupManagerView(category="Admin", name="Group Manager")

# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "group_manager", __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/group_manager')


# Defining the plugin class
class GroupManagerView(AirflowPlugin):
    name = "group_manager"
    flask_blueprints = [bp]
    admin_views = [v]
