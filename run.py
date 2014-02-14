#!/usr/bin/env python
# -*- coding: utf-8 -*-

import m3u8
import threading
import logging
from time import sleep

# Use a static URL during initial development
URI = 'http://www.nasa.gov/multimedia/nasatv/NTV-Public-IPS.m3u8'

logger = logging.getLogger('digest')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('spam.log')
fh.setLevel(logging.WARNING)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

def worker(playlist, m3u8_uri):
    '''the thread worker function'''
    logger.debug("worker for playlist: %s" % m3u8_uri)
    for segment in listen_segments(playlist, m3u8_uri):
        logger.debug('Found new segment %s' % segment)
    return

def load_playlist(m3u8_uri):
    '''load m3u8 URI and extract playlists '''
    logger.debug('Load playlist %s' % m3u8_uri)
    playlist = m3u8.load(m3u8_uri)
    if playlist.is_variant:
        return digest_variant_playlist(playlist, m3u8_uri)
    else:
        return digest_singel_playlist(playlist, m3u8_uri)

def digest_variant_playlist(playlist, m3u8_uri):
    '''extract single playlists from variant '''
    for p in playlist.playlists:
        load_playlist(p.absolute_uri)

def digest_singel_playlist(playlist, m3u8_uri):
    '''launch single playlist to a worker'''
    #TODO: add thread name (name=??, target=worker, args=(playlist,))
    t = threading.Thread(target=worker, args=(playlist, m3u8_uri,))
    t.setDaemon(True)
    threads.append(t)
    t.start()

def get_segments(m3u8_uri):
    '''extract all segments from a playlist and return it as a set'''
    segments = set()
    playlist = m3u8.load(m3u8_uri)
    segments = [segment.absolute_uri for segment in playlist.segments]
    return segments

def sort_set(data):
    '''Take a set as input, sort it alpha numeric and return the new set'''
    return sorted(data, key=lambda item: (int(item.partition(' ')[0])
                                    if item[0].isdigit() else float('inf'), item))

def listen_segments(playlist, m3u8_uri):
    old = set()
    while True:
        segments = set(get_segments(m3u8_uri))
        new = segments - old
        new = sort_set(new)
        for segment in new:
            yield segment
        duration = playlist.target_duration
        logger.debug('sleeping for %s seconds' % duration)
        sleep(duration)
        old = segments

if __name__ == "__main__":
    threads = []
    load_playlist(URI)
    while True:
        sleep(1)

