import os
import asyncio
import argparse
import getpass

from dev.src.hedgepy.server.bases import API_Instance


def main():
    root = os.getcwd()
    
    password = getpass.getpass('Enter password: ')
    api_instance = API_Instance(root=root, password=password)
    del password

    asyncio.run(api_instance.start())
    
    
if __name__ == '__main__':
    main()
    