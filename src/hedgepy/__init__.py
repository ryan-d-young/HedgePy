import os
import asyncio
import argparse
import getpass
from pathlib import Path
from hedgepy import api


ROOT = (Path(os.getcwd()) / 'src' / 'hedgepy').resolve()


def main():
    password = getpass.getpass('Enter password: ')
    api_instance = api.API(root=ROOT, password=password)
    del password
    
    asyncio.run(api_instance.start())
    
    
if __name__ == '__main__':
    main()
    