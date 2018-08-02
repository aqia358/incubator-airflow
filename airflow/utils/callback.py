# -*- coding: utf-8 -*-

import twilio
from twilio.rest import Client

# TWILIO_ACC_SID = 'AC3004f5f53719332b9dfb3b5389173bf9'
# TWILIO_ACC_TOKEN = '60cd4e877b9090b825f128bf00ee6bc5'
TWILIO_SENDER_ID = '+14156501023'
# TWILIO_VOICE_CFG = {0: ('default', 'https://handler.twilio.com/twiml/EHc6259a142800ec17a73bcb2ce4f67de2')}
# TWILIO_URL = 'https://handler.twilio.com/twiml/EHc6259a142800ec17a73bcb2ce4f67de2'

from airflow import configuration

TWILIO_ACC_SID = configuration.get('twilio', 'TWILIO_ACC_SID')
TWILIO_ACC_TOKEN = configuration.get('twilio', 'TWILIO_ACC_TOKEN')
TWILIO_URL = configuration.get('twilio', 'TWILIO_URL')
TWILIO_SENDER_ID = configuration.get('twilio', 'TWILIO_SENDER_ID')
client = Client(TWILIO_ACC_SID, TWILIO_ACC_TOKEN)


# target_number = 15101109515
# call = client.calls.create(to='15101109515', from_=TWILIO_SENDER_ID,
#                            url='https://handler.twilio.com/twiml/EHc6259a142800ec17a73bcb2ce4f67de2')
# call = client.calls.create(to='+8615101109515', from_=TWILIO_SENDER_ID,
#                            url='https://handler.twilio.com/twiml/EHc6259a142800ec17a73bcb2ce4f67de2')
# ms = client.messages.create(to='+8615101109515', from_=TWILIO_SENDER_ID, body='xxxxxx')


def sms(phone_number, body):
    if "+86" not in phone_number:
        phone_number = "+86" + phone_number
    ms = client.messages.create(to=phone_number, from_=TWILIO_SENDER_ID, body=body)


def call(phone_number):
    if "+86" not in phone_number:
        phone_number = "+86" + phone_number
    call = client.calls.create(to=phone_number, from_=TWILIO_SENDER_ID, url=TWILIO_URL)


'''
context:
{
            'dag': task.dag,
            'ds': ds,
            'ds_nodash': ds_nodash,
            'ts': ts,
            'ts_nodash': ts_nodash,
            'yesterday_ds': yesterday_ds,
            'yesterday_ds_nodash': yesterday_ds_nodash,
            'tomorrow_ds': tomorrow_ds,
            'tomorrow_ds_nodash': tomorrow_ds_nodash,
            'END_DATE': ds,
            'end_date': ds,
            'dag_run': dag_run,
            'run_id': run_id,
            'execution_date': self.execution_date,
            'prev_execution_date': prev_execution_date,
            'next_execution_date': next_execution_date,
            'latest_date': ds,
            'macros': macros,
            'params': params,
            'tables': tables,
            'task': task,
            'task_instance': self,
            'ti': self,
            'task_instance_key_str': ti_key_str,
            'conf': configuration,
            'test_mode': self.test_mode,
            'var': {
                'value': VariableAccessor(),
                'json': VariableJsonAccessor()
            }
        }
'''


def alert(context):
    try:
        dag = context.get('dag')
        ds = context.get('ds')
        json = context.get('var').get('json').get(dag)
        if not json:
            return
        sms_list = json.get('sms')
        for p in sms_list:
            sms(p, 'WARN [{}_{}] job failed.'.format(dag, ds))

        call_list = json.get('phone')
        for p in call_list:
            call(p)
    except Exception as e:
        print(e)
