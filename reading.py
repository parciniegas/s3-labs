from json import JSONEncoder
from dataclasses import dataclass

@dataclass
class Reading:
    raw_data: str
    ke: str
    pt: str
    ct: str
    noins: str
    id_socket: str
    id_client: str
    account_no: int
    kva_number: str
    custom_name: str
    channel: str
    uom: str
    date: str
    datetime: str
    utc_datetime: str
    dst_flag: bool
    no_intervals: int
    sf: str
    is_backup: bool
    num_log: int
    received_date: str
    origin_version: str
    validation_status: str
    register_date: str
    source: str
    version: str
    version_number: int
    id: str
    value: str

class ReadingEncoder(JSONEncoder):
    def default(self, data):
        return data.__dict__