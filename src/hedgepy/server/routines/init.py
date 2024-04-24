import getpass
from datetime import timedelta
from hedgepy.common.utils import config
from hedgepy.server.bases import Schedule, Server, Database


def make_server() -> Server.Server:
    return Server.Server()


def make_daemon(
    host: str, 
    port: int, 
    start: tuple[int, int, int], 
    stop: tuple[int, int, int], 
    interval: int
    ) -> Schedule.Daemon:
    start_td = timedelta(hours=start[0], minutes=start[1], seconds=start[2])
    stop_td = timedelta(hours=stop[0], minutes=stop[1], seconds=stop[2])
    interval_td = timedelta(seconds=interval)    
    return Schedule.Daemon(
        env={'SERVER_HOST': host,
             'SERVER_PORT': port, 
             'DAEMON_START': start_td,
             'DAEMON_STOP': stop_td,
             'DAEMON_INTERVAL': interval_td}
        )
    

def make_database(dbname: str, host: str, port: int, user: str, password: str) -> Database.Database:
    return Database.Database(
        dbname=dbname, 
        host=host, 
        port=port, 
        user=user, 
        password=password
        )


async def init() -> tuple[Server.Server, Database.Database, Schedule.Daemon]:
    server = make_server()
    
    dbpass = getpass.getpass(prompt='Enter database password: ')
    db = make_database(
        dbname=config.get('database', 'dbname'), 
        host=config.get('database', 'host'), 
        port=config.get('database', 'port'),
        user=config.get('database', 'user'),
        password=dbpass
        )
    del dbpass

    daemon = make_daemon(
        host=config.get('server', 'host'), 
        port=config.get('server', 'port'), 
        start=config.get('api', 'start'),
        stop=config.get('api', 'stop'),
        interval=config.get('api', 'interval')
        )

    return server, db, daemon
