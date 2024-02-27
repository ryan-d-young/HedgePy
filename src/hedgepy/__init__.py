import os
import asyncio
from hedgepy import api


root = os.getcwd()
api = api.API(root)


def main():
    asyncio.run(api.start())
    
    
if __name__ == '__main__':
    main()