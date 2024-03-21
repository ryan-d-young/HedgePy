import getpass
from pathlib import Path
from datetime import timedelta
from hedgepy.server.routines import dbinit, parse, process
from hedgepy.server.bases import Server, Database, Agent



def make_server(root: Path) -> Server.Server:
    return Server.Server(root)


def make_daemon(
    host: str, 
    port: int, 
    start: tuple[int, int, int], 
    stop: tuple[int, int, int], 
    interval: int
    ) -> Agent.Daemon:
    start_td = timedelta(hours=start[0], minutes=start[1], seconds=start[2])
    stop_td = timedelta(hours=stop[0], minutes=stop[1], seconds=stop[2])
    interval_td = timedelta(seconds=interval)    
    return Agent.Daemon(
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


async def init() -> tuple[Server.Server, Database.Database, Agent.Daemon, Agent.Schedule]:
    from hedgepy.common.utils import config
    
    server = make_server(config.SOURCE_ROOT)
    
    dbpass = getpass.getpass(prompt='Enter database password: ')
    db = make_database(
        dbname=config.get('database', 'dbname'), 
        host=config.get('database', 'host'), 
        port=config.get('database', 'port'),
        user=config.get('database', 'user'),
        password=dbpass
        )
    del dbpass

    await dbinit.dbinit(server, db, reset_first=True)
    
    daemon = make_daemon(
        host=config.get('server', 'host'), 
        port=config.get('server', 'port'), 
        start=config.get('api', 'start'),
        stop=config.get('api', 'stop'),
        interval=config.get('api', 'interval')
        )

    schedule = parse.parse(daemon_start=config.get('api', 'start'), daemon_stop=config.get('api', 'stop'))
        
    return server, db, daemon, schedule
