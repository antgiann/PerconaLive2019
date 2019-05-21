#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#Variables, change accordinatly
chunksize=64
numofshards=6
groups=32

shards={}
shards_with_groups={}
groups_by_count={}
groups_by_source={}
groups_by_destination={}
actionlog={"ChunkMove":0}

# list of lists [<size>,<bounds>,<bounds>,<shard>,<group>]
chunks_list=[[36, 0, 0, 's2'], [55, 0, 0, 's3'], [45, 0, 0, 's4'], [32, 0, 0, 's5'], [41, 0, 0, 's6'], [46, 0, 0, 's1'], [35, 0, 0, 's2'], [46, 0, 0, 's3'], [55, 0, 0, 's4'], [49, 0, 0, 's5'], [39, 0, 0, 's6'], [42, 0, 0, 's1'], [36, 0, 0, 's2'], [37, 0, 0, 's3'], [43, 0, 0, 's4'], [55, 0, 0, 's5'], [53, 0, 0, 's6']]

# Adds a group number as element [4] on each chunk
def TagChunks():
    for item in chunks_list:
        item.append(int((item[0]-(chunksize/2))//(chunksize/(groups*2))))

#For each shard calculates the size [3] is the shard name and [0] the size        
def PopulateShards():
    shards.clear()
    for item in chunks_list:
        try:
            shards[item[3]] = shards[item[3]]+item[0]
        except KeyError:
         shards[item[3]]=item[0]
    return shards

#For each shard it calculates the amount of groups.A dictionary with key=shard and value a list with groups
def PopulateShardsGroups():
    shards_with_groups.clear()
    for item in chunks_list:
        try:
            shards_with_groups[item[3]].append(item[4])
        except KeyError:
         shards_with_groups[item[3]]=[item[4]]
    return shards_with_groups

#For each group it calculates the count of groups.A dictionary with key=group and value its overall number
def Groupsbycount():
    for item in chunks_list:
         try:
            groups_by_count[item[4]] = groups_by_count[item[4]]+1
         except KeyError:
            groups_by_count[item[4]]=1
    return groups_by_count
        
#Finds which groups must donate and which must receive
def FindDonorsandRecipients():
 for group in range(0,groups):
    try:
     for key in shards_with_groups:
         if (shards_with_groups[key].count(group))>=int(groups_by_count[group]/numofshards)+1:
           for z in range(0,shards_with_groups[key].count(group)-int(groups_by_count[group]/numofshards)):
             if group in groups_by_source:
                groups_by_source[group].append(key)
             else:
                 groups_by_source[group]=[key]      
         elif (shards_with_groups[key].count(group)<(int(groups_by_count[group]/numofshards)+1)):
            for z in range(0,int(groups_by_count[group]/numofshards)+1 - (shards_with_groups[key].count(group))):
             if group in groups_by_destination:
               groups_by_destination[group].append(key)
             else:
               groups_by_destination[group]=[key]
         else:
          pass             
    except KeyError:
        pass

def FindBestCase():
    avgvalue=0
    for item in chunks_list:
        avgvalue=avgvalue+item[0]
    return avgvalue/numofshards

def PrintDifference():
    optimal=FindBestCase()
    PopulateShards()
    avgdiff=0
    for element in shards:
        avgdiff=avgdiff+round(abs(shards[element]-optimal),1)
        print("Shard" ,element, "is", round(abs(shards[element]-optimal),1) , "MiB away from the optimal usage")
    print("The average difference is:",round(avgdiff/numofshards,1))

def MainFunction():
    for i in range(0,groups):
     try:
       counter=0
       if groups_by_source[i]<groups_by_destination[i]:
           for element in groups_by_source[i]:
               try:
                for chunk in chunks_list:
                    if (chunk[3]==element and chunk[4]==i):
                        chunk[3]=groups_by_destination[i][counter]
                        actionlog["ChunkMove"]=actionlog["ChunkMove"]+1
                        break
                counter=counter+1
               except IndexError:
                   pass
       else:
           for element in groups_by_destination[i]:
               try:
                 for chunk in chunks_list:
                    if (chunk[3]==groups_by_source[i][counter] and chunk[4]==i):
                        chunk[3]=element
                        actionlog["ChunkMove"]=actionlog["ChunkMove"]+1
                        break
                 counter=counter+1
               except IndexError:
                   pass
     except KeyError:
      pass

#execution starts here
TagChunks() 
PopulateShards()   
PopulateShardsGroups()
Groupsbycount()
FindDonorsandRecipients()
print("Shard balance berfore:")
PrintDifference()
MainFunction()
PopulateShards()
print("")
print("Shard balance after:")
PrintDifference()
print("Number of ChunkMoves",actionlog["ChunkMove"])

