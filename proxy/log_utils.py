#!/usr/bin/env python
#
#  utils.py
#
#  Copyright 2011 SinaEdge. All rights reserved.
#
import logging
import ScribeHandler

import settings

# from scribe import scribe
# from thrift.transport import TTransport, TSocket
# from thrift.protocol import TBinaryProtocol
#
# def log_to_scribe(message, host='localhost', port=1463, category=''):
#     log_entry = scribe.LogEntry(category=category, message=message)
#
#     socket = TSocket.TSocket(host=host, port=port)
#     transport = TTransport.TFramedTransport(socket)
#     protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
#     client = scribe.Client(iprot=protocol, oprot=protocol)
#
#     try:
#         transport.open()
#         result = client.Log(messages=[log_entry])
#         transport.close()
#         if result != scribe.ResultCode.OK:
#             logging.error('Error when log to Scribe')
#     except TTransport.TTransportException as ex:
#         logging.error('Error when log to Scribe: %s' % ex.message)

def setup_logger():
    collect_logger = logging.getLogger('Collect_Profile')
    collect_logger.setLevel(logging.DEBUG)
    collect_logger.propagate = False
    collect_handler = ScribeHandler.ScribeHandler(
                        category=settings.SCRIBE_CATEGORY_PLAYBENCH,
                        host=settings.SCRIBE_HOST,
                        port=settings.SCRIBE_PORT,
                    )
    collect_logger.addHandler(collect_handler)

    # instant_logger = logging.getLogger('InstantLogger')
    # instant_logger.setLevel(logging.DEBUG)
    # instant_logger.propagate = False
    # instant_handler = ScribeHandler.ScribeHandler(
    #                       category=settings.SCRIBE_CATEGORY_INSTANT,
    #                       host=settings.SCRIBE_HOST,
    #                       port=settings.SCRIBE_PORT,
    #                   )
    # instant_logger.addHandler(instant_handler)

    # status_logger = logging.getLogger('StatusLogger')
    # status_logger.setLevel(logging.DEBUG)
    # status_handler = ScribeHandler.ScribeHandler(
    #                       category=settings.SCRIBE_CATEGORY_STATUS,
    #                       host=settings.SCRIBE_HOST,
    #                       port=settings.SCRIBE_PORT,
    #                   )
    # status_logger.addHandler(status_handler)

setup_logger()

def log_to_scribe(message, category='collect_profile'):
    """ category values: collect_profile"""
    if category == 'collect_profile':
        logger = logging.getLogger('Collect_Profile')
    else:
        return

    logger.info(message)

if __name__ == '__main__':
    logger = logging.getLogger('Collect_Profile')
    logger.info('a')
