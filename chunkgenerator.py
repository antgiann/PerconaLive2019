#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random

shards=["s1","s2","s3","s4","s5","s6"]
chunks=[]
for number in range(1,18):
  locallist=[]
  locallist.append(random.randint(32,63))
  locallist.append(0)
  locallist.append(0)
  locallist.append(shards[number%len(shards)])
  chunks.append(locallist)
