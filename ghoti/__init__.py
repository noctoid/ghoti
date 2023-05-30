from pydantic import BaseModel


class DBServerURI(BaseModel):
    host: str
    port: int
    db: str
    user: str
    password: str


class Connection:
    def __init__(server: DBServerURI):
        self.connection = None


class Payload(BaseModel):
    q_sequence_id: int


class Message(BaseModel):
    q_sequence_id: int
    channel: str
    extra: dict
    payload: list[Payload]


def connect(server: DBServerURI) -> Connection:
    pass


def pub(connection: Connection, message: Message):
    pass


def sub(connection: Connection, start_from: int, limit: int) -> list[Message]:
    pass

