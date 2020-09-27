import json
import socket
from dataclasses import asdict, dataclass
from typing import Dict, List

Media = Dict[str, str]


@dataclass
class Status:
    ongoing: bool
    complete: bool
    percentage: float
    message: str
    errors: List[str]
    media: Dict[str, Media]


class Connection:
    def __init__(self, sockfile: str):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.connect(sockfile)
        self.last_status = Status(
            ongoing=False,
            complete=False,
            percentage=0,
            message="",
            errors=[],
            media=[],
        )

    def send(self, import_status: Status):
        self.last_status = import_status
        self.socket.send(json.dumps(asdict(import_status)).encode())
        self.socket.send(b"\n")

    def start(
        self,
    ):
        self.send(
            Status(
                ongoing=True,
                complete=False,
                percentage=0,
                message="getting things ready...",
                errors=[],
                media=[],
            )
        )

    def progress(self, percentage, message):
        self.send(
            Status(
                ongoing=True,
                complete=False,
                percentage=percentage,
                message=message,
                errors=[],
                media=[],
            )
        )

    def message(self, message):
        self.last_status.message = message
        self.send(self.last_status)

    def finish(self, media, errors):
        self.send(
            Status(
                ongoing=False,
                complete=True,
                percentage=1,
                message="Done!",
                media=media,
                errors=errors,
            )
        )
        self.socket.close()
