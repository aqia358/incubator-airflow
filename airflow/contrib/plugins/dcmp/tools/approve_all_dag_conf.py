# encoding: utf-8

import os
import sys
sys.path.append( os.path.join(os.path.dirname(__file__), '..', '..') )

import airflow
import datetime
from dcmp.models import DcmpDag, DcmpDagConf
from dcmp.dag_converter import dag_converter
from airflow.utils.db import provide_session
from airflow import models, settings
from airflow.exceptions import AirflowException
from sqlalchemy import or_

@provide_session
def main(session=None):
    dcmp_dags = session.query(DcmpDag).order_by(DcmpDag.dag_name).all()
    for dcmp_dag in dcmp_dags:
        if dcmp_dag.version != dcmp_dag.approved_version:
            dcmp_dag.approve_conf(session=session)
    session.commit()

    check_date = datetime.datetime.now() - datetime.timedelta(minutes=1)

    dags_to_delete =  session.query(DcmpDagConf).filter(
        DcmpDagConf.action == "delete",
        DcmpDagConf.created_at >= check_date
    ).all()
    for dag in dags_to_delete:
        count = delete_dag.delete_dag(dag.dag_name)
        print("Removed {} record(s)".format(count))
    dag_converter.refresh_dags()

def delete_dag(dag_id):
    session = settings.Session()

    DM = models.DagModel
    dag = session.query(DM).filter(DM.dag_id == dag_id).first()
    if dag is None:
        print("Dag id {} not found".format(dag_id))

    dagbag = models.DagBag()
    if dag_id in dagbag.dags:
        print("Dag id {} is still in DagBag. "
                            "Remove the DAG file first.".format(dag_id))

    count = 0

    for m in models.Base._decl_class_registry.values():
        if hasattr(m, "dag_id"):
            cond = or_(m.dag_id == dag_id, m.dag_id.like(dag_id + ".%"))
            count += session.query(m).filter(cond).delete(synchronize_session='fetch')

    if dag.is_subdag:
        p, c = dag_id.rsplit(".", 1)
        for m in models.DagRun, models.TaskFail, models.TaskInstance:
            count += session.query(m).filter(m.dag_id == p, m.task_id == c).delete()

    session.commit()

    return count


if __name__ == "__main__":
    main()
