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

from __future__ import unicode_literals

from sys import version_info

import base64
import flask_login
from flask_login import login_required, current_user, logout_user
from flask import flash, Response
from wtforms import Form, PasswordField, StringField
from wtforms.validators import InputRequired
from functools import wraps

from flask import url_for, redirect, make_response
from flask_bcrypt import generate_password_hash, check_password_hash

from sqlalchemy import Column, String
from sqlalchemy.ext.hybrid import hybrid_property

from airflow import settings
from airflow import models
from airflow.models import User
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() below
login_manager.login_message = None

log = LoggingMixin().log
PY3 = version_info[0] == 3


class AuthenticationError(Exception):
    pass

@provide_session
def group_users(user, session=None):
    if user:
        users = session.query(models.User).filter(models.User.group_name == user.group_name).all()
        return [u.username for u in users]
    else:
        return ''

class WebAuthUser(models.User):

    def __init__(self, user):
        self.user = user
        self.group_users = group_users(user)

    def is_active(self):
        '''Required by flask_login'''
        return True

    def is_authenticated(self):
        '''Required by flask_login'''
        return True

    def is_anonymous(self):
        '''Required by flask_login'''
        return False

    def get_id(self):
        '''Returns the current user id as required by flask_login'''
        return str(self.id)

    def data_profiling(self):
        '''Provides access to data profiling tools'''
        return True

    def is_superuser(self):
        '''Access all the things'''
        if self.user:
            return self.user.superuser
        else:
            return False


@login_manager.user_loader
@provide_session
def load_user(userid, session=None):
    log.debug("Loading user %s", userid)
    if not userid or userid == 'None':
        return None

    user = session.query(models.User).filter(models.User.id == int(userid)).first()
    return WebAuthUser(user)


def authenticate(session, username, group):
    """
    Authenticate a WebAuthUser with the specified
    username/password.

    :param session: An active SQLAlchemy session
    :param username: The username
    :param password: The password

    :raise AuthenticationError: if an error occurred
    :return: a WebAuthUser
    """
    if not username:
        raise AuthenticationError()

    user = session.query(WebAuthUser).filter(
        WebAuthUser.username == username).first()

    if not user:
        user = WebAuthUser(User())
        user.username = username
        user.group_name = group
        session.add(user)
        session.commit()
       # raise AuthenticationError()

    log.info("User %s successfully authenticated", username)
    return user


@provide_session
def login(self, request, session=None):
    if current_user.is_authenticated():
        flash("You are already logged in")
        return redirect(url_for('admin.index'))

    username = request.headers.get('webauth-username')
    group_list = request.headers.get('webauth-group')

    form = LoginForm(request.form)
    try:
        user = authenticate(session, username, group_list)
        flask_login.login_user(user)

        return redirect(request.args.get("next") or url_for("admin.index"))
    except AuthenticationError:
        flash("Incorrect login details")
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)
    finally:
        session.commit()
        session.close()


class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])


def _unauthorized():
    """
    Indicate that authorization is required
    :return:
    """
    return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic"})


def _forbidden():
    return Response("Forbidden", 403)


def init_app(app):
    pass

