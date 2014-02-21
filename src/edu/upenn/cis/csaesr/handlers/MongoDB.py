"""A handler to access and modify a mongodb instance
"""
# Copyright (C) 2014 Taylor Turpen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from pymongo import MongoClient
from data.structures import Turker, AudioClip, Transcription, AudioSource, HitEntity
from handlers.Exceptions import MultipleResultsOnIdFind, TooManyEntries
from time import time
from collections import defaultdict
import logging

class MongoHandler(object):
    default_db_loc = 'mongodb://localhost:27017'

    def __init__(self):
        client = MongoClient(self.default_db_loc)
        self.queue_revive_time = 5       
        self.db = client.transcription_db
        self.audio_sources = self.db.audio#The full audio files
        self.audio_clips = self.db.audio_clips#portions of the full audio files
        self.audio_clip_queue = self.db.audio_clip_queue#queue of the audio clips
        self.transcripts = self.db.transcripts
        self.transcription_hits = self.db.transcription_hits
        self.disfluency_hits = self.db.disfluency_hits
        self.second_pass_hits = self.db.second_pass_hits
        self.turkers = self.db.turkers
        self.type_ids = self.db.type_ids
        
        self.logger = logging.getLogger("transcription_engine.mongodb_handler")
        self.logger.info("creating")
        
    def insert_entity(self,entity):
        entry = entity.get_mongo_entry()
        if isinstance(entity,Turker):
            self.turkers.insert(entry)
        elif isinstance(entity,AudioClip):
            self.audio_clips.insert(entry)
        elif isinstance(entity,Transcription):
            self.transcripts.insert(entry)
        elif isinstance(entity,AudioSource):
            self.audio_sources.insert(entry)
    
    def get_hit_by_id(self,hit_id):
        hits = self.transcripts.find({"_id" : hit_id})
        if len(hits) > 1:
            raise MultipleResultsOnIdFind(hit_id)
        hit = HitEntity(hits[0])
        return hit
    
    def get_type_ids(self):
        """All data types should have a type ID"""
        return self.type_ids.find({}) 
    
    def audio_clip_find_one(self,search,field):
        response = [w for w in self.audio_clips.find(search,{field : 1})]
        if len(response) != 1:
            raise TooManyEntries
        return response[0][field]
    
    def get_audio_clip_url(self,audio_clip_id):
        return self.audio_clip_find_one({"_id" : audio_clip_id},"audio_clip_url")
    
    def get_audio_clip_status(self,audio_clip_id):
        return self.audio_clip_find_one({"_id" : audio_clip_id},"status")
    
    def get_max_queue(self,max_sizes):
        max_q = []
        for q in max_sizes:
            #For each q in each q size
            if len(max_sizes[q]) >= q:
                #if the q is full
                if q > len(max_q):
                    #if the q is bigger than the q with the most clips
                    max_q = max_sizes[q][:q]
        return max_q
    
    def revive_audio_clip_queue(self):
        """If a audio clip in the queue has been processing for more than
            queue_revive_time seconds, reset its processing status"""
        non_none = self.audio_clip_queue.find({"processing": {"$ne" : None}})
        for clip in non_none:
            if  time() - clip["processing"] > self.queue_revive_time:
                self.audio_clip_queue.update({"_id":clip["_id"]}, {"$set" : {"processing" : None}})  
        self.logger.info("Finished reviving audio clip queue.")
    
    def update_audio_clip_queue(self,clip_queue):
        """Remove the audio clip entries from the clip queue"""
        for clip in clip_queue:
            self.audio_clip_queue.remove({"_id":clip["_id"]})      
        self.logger.info("Finished updating audio clip queue") 
            
    def update_audio_clip_status(self,clip_id_list,new_status):
        if type(clip_id_list) != list:
            self.logger.error("Error updating audio clip(%s)"%clip_id_list)
            raise IOError
        for clip_id in clip_id_list:
            self.audio_clips.update({"_id":clip_id},  {"$set" : {"status" : new_status}}  )   
        self.logger.info("Updated audio clip status for: %s"%clip_id_list)
        return True
    
    def update_transcription_hit_status(self,hit_id,hit_type_id,clip_queue,new_status):
        if type(clip_queue) != list:
            raise IOError
        clips = [w["audio_clip_id"] for w in clip_queue]
        self.transcription_hits.update({"_id":hit_id},
                                       {"_id":hit_id,
                                        "hit_type_id": hit_type_id,
                                        "clips" : clips,
                                        "status": new_status},
                                       upsert = True)
        self.logger.info("Updated transcription hit %s "%hit_id)
        return True
    
    def remove_transcription_hit(self,hit_id):
        self.transcription_hits.remove({"_id":hit_id})
                
    def get_audio_clip_pairs(self,clip_queue):
        return [(self.get_audio_clip_url(w["audio_clip_id"]),w["audio_clip_id"]) for w in clip_queue]
    
    def queue_clip(self,audio_clip_id,priority=1,max_queue_size=3):
        self.audio_clip_queue.update({"audio_clip_id": audio_clip_id},
                             {"audio_clip_id": audio_clip_id,
                              "priority": priority,
                              "max_size": max_queue_size,
                              "processing" : None,
                              },
                             upsert = True)
        self.update_audio_clip_status([audio_clip_id], "Queued")
        self.logger.info("Queued clip: %s "%audio_clip_id)
        
    def get_audio_clip_queue(self):
        """Insert the audio clip by id into the queue.
            Get all the clips waiting in the queue and not being processed
            Find the largest queue that is full
            Update the queue and return the clips"""            
        self.revive_audio_clip_queue()
        queue = self.audio_clip_queue.find({"processing":None})
        max_sizes = defaultdict(list)
        for clip in queue:
            id = clip["_id"]
            clip_id = clip["audio_clip_id"]
            priority = clip["priority"]
            processing = clip["processing"]
            max_size = int(clip["max_size"])
            for i in range(max_size):
                max_sizes[i+1].append(clip)
        max_q = self.get_max_queue(max_sizes)        
        for clip in max_q:
            t = time()
            self.audio_clip_queue.find_and_modify(query = {"_id":clip["_id"]},
                                                 update = { "$set" : {"processing":t}}
                                                )
        return max_q
    
def main():
    mh = MongoHandler()
    t = Turker(5)
    print(t.get_mongo_entry())
    mh.insert_entity(t)
    
if __name__ == "__main__":
    main()
        
       
        
        
    