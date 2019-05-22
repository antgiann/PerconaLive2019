#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#Static Variables
current=0
chunksize=64

actionlog={"ChunkMove":0, "ChunkMerge":0, "ChunkSplit":0}
shards = {}
chunks_list = [[10,"minkey",0,"shard1"],[5,0,10,"shard2"],[20,10,20,"shard1"],[30,20,30,"shard2"],
              [25,40,500,"shard2"],[7,40,50,"shard3"],[45,50,60,"shard3"], [35,60,70,"shard3"], 
              [5,70,80,"shard3"],[15,80,90,"shard2"],[15,90,100,"shard1"],[55,100,"maxkey","shard1"]]
init_list=chunks_list.copy()
final_list=[]

def PopulateShards():
    shards.clear()
    for item in chunks_list:
        try:
            shards[item[3]] = shards[item[3]]+item[0]
        except KeyError:
            shards[item[3]]=item[0]

def moveChunk(source,destination):
    actionlog["ChunkMove"]= actionlog["ChunkMove"]+1

def splitChunk():
    for item in chunks_list:
        if item[0]>chunksize:
            final_list.append([item[0]/2, item[1], "TBD", item[3]])
            final_list.append([item[0]/2, "TBD", item[2], item[3]])
            actionlog["ChunkSplit"]= actionlog["ChunkSplit"]+1
        else:
            final_list.append(item)

def MergeChunk(current,shard):
    chunks_list[current]=[chunks_list[current][0]+chunks_list[current+1][0],chunks_list[current][1],chunks_list[current+1][2],shard]
    chunks_list.remove(chunks_list[current+1])
    actionlog["ChunkMerge"]= actionlog["ChunkMerge"]+1
    
def PrintPrettyChunkMap():
    chunkspershard={}
    for item in final_list:
        try:
            chunkspershard[item[3]]=[]
        except KeyError:
            chunkspershard[item[3]]=[]
    for item in final_list:
        chunkspershard[item[3]].append(item[0])
    return chunkspershard

#Populate the chunk 
PopulateShards()

while current<len(chunks_list)+1:
    try:
     #If the two chunks don't produce a jumbo (64 Min) first position of the list is the size  
     if (chunks_list[current][0]+chunks_list[current+1][0]<=chunksize):
         #If the two chunks are in the same shard
         if chunks_list[current][3]==chunks_list[current+1][3]:
            MergeChunk(current,chunks_list[current][3])
         #If the two chunks are on different shards pick the shard with less data to move and then merge    
         else:
            if (shards[chunks_list[current][3]]>shards[chunks_list[current+1][3]]):
                #UpdateChunkDistribution(current,0)
                moveChunk(chunks_list[current],chunks_list[current+1])
                MergeChunk(current,chunks_list[current+1][3])
                PopulateShards()
            else:
               #UpdateChunkDistribution(current,1)
               moveChunk(chunks_list[current],chunks_list[current+1])
               MergeChunk(current,chunks_list[current][3])
               PopulateShards()
     #If the two chunks produce a jumbo then push it up to 128 MiB (2*chunksize)          
     elif (chunks_list[current][0]+chunks_list[current+1][0]>chunksize and chunks_list[current][0]+chunks_list[current+1][0]<chunksize*2):
         #again if they are in the same shard
         if chunks_list[current][3]==chunks_list[current+1][3]:
                MergeChunk(current,chunks_list[current][3])
         #again if they are not in the same shard, always move to current shard as current+1 is never a jumbo      
         else:
                #UpdateChunkDistribution(current,0)
                moveChunk(chunks_list[current],chunks_list[current+1])
                MergeChunk(current,chunks_list[current][3])
                PopulateShards()
     #exceeding 128MiB so just move pointer to next chunk           
     else:
         current=current+1
    except IndexError:
        break

splitChunk()
print("===Actions performed during the execution===")
for item in actionlog:
    print(item ,actionlog[item])
print("")
print("===Chunk Map Before Moves/Merges/Splits===")
print(init_list)
print("")
print("===Chunk Map After Moves/Merges/Splits===")
print(final_list)
print("")
print("===Shard Usage After Moves/Merges/Splits===")
print(shards)
print("")
print("===Chunks per Shard After Moves/Merges/Splits===")
print(PrintPrettyChunkMap())
