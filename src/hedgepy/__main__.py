import os
import asyncio
import argparse
import getpass
from pathlib import Path
from hedgepy import api


def main():
    root = os.getcwd()
    password = getpass.getpass('Enter password: ')
    api_instance = api.API_Instance(root=root, password=password)
    del password
    
    
    asyncio.run(api_instance.start())
    
    
if __name__ == '__main__':
    main()
    