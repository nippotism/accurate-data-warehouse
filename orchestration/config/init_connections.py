from airflow.models import Connection
from airflow import settings
import yaml

session = settings.Session()

with open('/opt/airflow/config/connections.yaml') as f:
    data = yaml.safe_load(f)

for conn in data['connections']:
    if not session.query(Connection).filter_by(conn_id=conn['conn_id']).first():
        session.add(Connection(**conn))

session.commit()
session.close()