# -*- coding: utf-8 -*-

import os
import sys

os.chdir(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(r'../'))

from hagworm.extend.asyncio.base import Utils


async def test(name):

    Utils.log.info(r'waiting: {0}'.format(name))

    await Utils.sleep(5)

    Utils.log.info(r'finished')


def main():

    Utils.run_until_complete(test, r'wsb')


if __name__ == r'__main__':

    main()
