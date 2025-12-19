import configparser
import pymysql
from pathlib import Path

CFG_PATH = Path(__file__).resolve().parent / "config.ini"

def get_conn():
    cfg = configparser.ConfigParser()
    cfg.read(CFG_PATH, encoding="utf-8")
    c = cfg["mysql"]

    return pymysql.connect(
        host=c.get("host", "127.0.0.1"),
        port=c.getint("port", 3306),
        user=c.get("user", "root"),
        password=c.get("password", "123456"),
        database=c.get("database", "job_pricing"),
        charset=c.get("charset", "utf8mb4"),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
