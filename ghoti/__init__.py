import json
from uuid import uuid4
from datetime import datetime
from decimal import Decimal
from contextlib import contextmanager
from pydantic import BaseModel, Field, validator, UUID4
from clickhouse_driver import Client


class DBServerURI(BaseModel):
    host: str
    port: int
    db: str
    user: str
    password: str


class Payload(BaseModel):
    msg_id: datetime

    class Config:
        orm_mode = True


class TestPayload(Payload):
    a_string: str
    b_int: int
    c_bool: bool
    d_float: float
    e_datetime: datetime
    f_decimal: Decimal


class Message(BaseModel):
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime | None
    channel: str
    trace_id: UUID4 = Field(default_factory=uuid4)
    extra: dict
    payload: list[Payload]

    class Config:
        orm_mode = True


# @contextmanager
def connect(uri: DBServerURI):
    return Client.from_url(f"clickhouse://{uri.user}:{uri.password}@{uri.host}:{uri.port}/{uri.db}")
    # try:
    #     yield client
    # finally:
    #     client.disconnect()

    # connection = None
    # try:
    #     match uri.driver:
    #         case "clickhouse":
    #             uri_str = f"clickhouse://{uri.user}:{uri.password}@{uri.host}:{uri.port}/{uri.db}"
    #             connection = clickhouse_driver.dbapi.connect(uri_str)
    #         case _:
    #             connection = None
    #     yield connection
    # finally:
    #     connection.commit()
    #     connection.close()


def q_setup(connection: Client):
    connection.execute('''
        CREATE TABLE q (
            created_at DateTime64 default now(),
            updated_at DateTime64 default 0,
            trace_id UUID,
            channel String,
            extra String
        ) ENGINE = MergeTree() ORDER BY (created_at)
        ''')


def payload_schema_to_columns(payload: Payload):
    schema = payload.schema()
    return ','.join(schema['properties'].keys())


def pub(connection: Client, message: Message):
    channel = f"channel_{message.channel}"

    print(connection.execute(f"""
        INSERT INTO {channel} ({payload_schema_to_columns(message.payload[0])}) VALUES 
    """, [p.dict() for p in message.payload]))
    print(connection.execute(f"""
        INSERT INTO q (created_at, updated_at, trace_id, channel, extra) VALUES 
    """, {'created_at': message.created_at, 'trace_id': message.trace_id,
          'channel': message.channel, 'extra': json.dumps(message.extra)}
    ))


def sub(connection: Client, start_from: int, limit: int) -> list[Message]:
    pass
