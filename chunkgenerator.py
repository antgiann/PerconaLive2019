#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random

#num of shards
numofshards=6
#Range min/max for the size of chunks
minsize=0
maxsize=64
#Number of chunks
numofchunks=20

#Populate shard map:
shards=[]
for number in (0,numofshards):
    shards.append("s"+str(number))

#Popoulate chunk map:
chunks=[]
for number in range(0,numofchunks):
  locallist=[]
  locallist.append(random.randint(minsize,maxsize))
  locallist.append(number)
  locallist.append(number+1)
  locallist.append(shards[number%len(shards)])
  chunks.append(locallist)

#Fix minKey, maxKey
chunks[0][1]="minKey"
chunks[number][2]="maxKey"

print("===ChunkMap===")
print(chunks)
