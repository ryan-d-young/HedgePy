import os
from pathlib import Path
import asyncio
from hedgepy import api


root = (Path(os.getcwd()) / 'src' / 'hedgepy').resolve()
api = api.API(root)


def main():
    asyncio.run(api.start())
    
    
if __name__ == '__main__':
    main()
    