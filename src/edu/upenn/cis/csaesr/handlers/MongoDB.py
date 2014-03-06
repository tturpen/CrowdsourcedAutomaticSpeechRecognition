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
from handlers.Exceptions import MultipleResultsOnIdFind, TooManyEntries, DuplicateArtifactException
from time import time
from collections import defaultdict
from bson.objectid import ObjectId

import logging

class MongoHandler(object):
    default_db_loc = 'mongodb://localhost:27017'

    def __init__(self):
        client = MongoClient(self.default_db_loc)
        self.queue_revive_time = 5       
        self.db = client.transcription_db
        self.c = {}#dictionary of collections
        self.c["audio_sources"] = self.db.audio#The full audio files
        self.c["audio_clips"] = self.db.audio_clips#portions of the full audio files
        self.c["audio_clip_queue"] = self.db.audio_clip_queue#queue of the audio clips
        self.c["transcriptions"] = self.db.transcriptions
        self.c["reference_transcriptions"] = self.db.reference_transcriptions
        self.c["transcription_hits"] = self.db.transcription_hits
        self.c["disfluency_hits"] = self.db.disfluency_hits
        self.c["second_pass_hits"] = self.db.second_pass_hits
        self.c["turkers"] = self.db.turkers
        self.c["assignments"] = self.db.assignments
        self.c["workers"] = self.db.workers
        
        self.logger = logging.getLogger("transcription_engine.mongodb_handler")
        
    def get_artifact_by_id(self,collection,art_id,field=None,refine={}):
        return self.get_artifact(collection,{"_id":ObjectId(art_id)},field,refine)
    
    def get_artifacts_by_state(self,collection,state,field=None,refine={}):
        return self.c[collection].find({"state":state})
    
    def get_artifacts(self,collection,param,values,field=None,refine={}):
        return [self.get_artifact(collection,{param:val},field,refine) for val in values]
    
    def get_artifact(self,collection,search,field=None,refine={}):
        responses = self.c[collection].find(search,refine)\
                    if refine else self.c[collection].find(search)
        prev = False
        for response in responses:
            if prev:
                raise TooManyEntries("MongoHandler.get_artifact."+collection)
            prev = response        
        return prev[field] if field and prev else prev
    
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
        """If an audio clip in the queue has been processing for more than
            queue_revive_time seconds, free the clip by resetting processing"""
        non_none = self.c["audio_clip_queue"].find({"processing": {"$ne" : None}})
        for clip in non_none:
            if  time() - clip["processing"] > self.queue_revive_time:
                self.c["audio_clip_queue"].update({"_id":clip["_id"]}, {"$set" : {"processing" : None}})  
        self.logger.info("Finished reviving audio clip queue.")
    
    def remove_artifacts(self,collection,artifacts):
        """Given a list of artifacts, remove each one by _id"""
        for artifact in artifacts:
            self.c[collection].remove({"_id":artifact["_id"]})
    
    def remove_artifacts_by_id(self,collection,artifact_ids):
        """Given a list of artifacts, remove each one by _id"""
        for artifact_id in artifact_ids:
            self.c[collection].remove({"_id":artifact_id})
            
    def update_artifacts_by_id(self,collection,artifact_ids,field,value):
        """Given a list of artifact ids, update each one's field to value"""
        if type(artifact_ids) != list:
            self.logger.error("Error updating %s audio clip(%s)"%(collection,artifact_ids))
            raise IOError
        for artifact_id in artifact_ids:
            self.c[collection].update({"_id":artifact_id}, {"$set" : {field:value}})            
            
    def remove_audio_clips_from_queue(self,clip_queue):
        """Remove the audio clip entries from the clip queue"""
        self.remove_artifacts("audio_clip_queue",clip_queue)
        self.logger.info("Finished updating audio clip queue") 
            
    def update_audio_clips_state(self,clip_id_list,new_state):
        if type(clip_id_list) != list:
            self.logger.error("Error updating audio clip(%s)"%clip_id_list)
            raise IOError
        self.update_artifacts_by_id("audio_clips",clip_id_list,"state",new_state)
        self.logger.info("Updated audio clip state for: %s"%clip_id_list)
        return True
    
    def upsert_artifact(self,collection,search,document):
        """Artifacts that have external imposed unique IDs
            can be created with an upsert."""            
        self.c[collection].update(search,document,upsert = True)
        
    def create_transcription_hit_artifact(self,hit_id,hit_type_id,clip_queue,new_state):
        if type(clip_queue) != list:
            raise IOError
        clips = [w["audio_clip_id"] for w in clip_queue]
        document =  {"_id":hit_id,
                     "hit_type_id": hit_type_id,
                     "clips" : clips,
                     "state": new_state}
        self.upsert_artifact("transcription_hits",{"_id": hit_id},document)
        self.logger.info("Updated transcription hit %s "%hit_id)
        return True
    
    def create_assignment_artifact(self,assignment,transcription_ids,state):
        """Create the assignment document with the transcription ids.
            AMTAssignmentStatus is the AMT assignment state.
            state is the engine lifecycle state."""
        assignment_id = assignment.AssignmentId
        document = {"_id":assignment_id,
                     "AcceptTime": assignment.AcceptTime,
                     "AMTAssignmentStatus" : assignment.AssignmentStatus,
                     "AutoApprovalTime" : assignment.AutoApprovalTime,
                     "hit_id" : assignment.HITId,
                     "worker_id" : assignment.WorkerId,
                     "transcriptions" : transcription_ids,
                     "state": state}
        self.upsert_artifact("assignments", {"_id": assignment_id},document)
        self.logger.info("Created Assignment document, assignment ID(%s) "%assignment_id)
        return True
    
    def create_audio_source_artifact(self,uri,disk_space,length_seconds,
                                     sample_rate,speaker_id,
                                     encoding,audio_clip_id=None,state="New"):
        """Each audio source is automatically an audio clip,
            Therefore the reference transcription can be found
            by referencing the audio clip assigned to this source.
            For turkers, speaker id will be their worker id"""        
        document = {"uri" : uri,
                    "disk_space" : disk_space,
                    "length_seconds" : length_seconds,
                    "audio_clip_id" : audio_clip_id,
                    "sample_rate" : sample_rate,
                    "speaker_id" : speaker_id,
                    "encoding" : encoding,
                    "state" : state}
        #soft source checking
        self.upsert_artifact("audio_sources", document, document)
        return self.get_artifact("audio_sources",document,field="_id")
        
    def create_reference_transcription_artifact(self,audio_clip_id,words,level):
        """Create the reference transcription given the audio clip id
            and the transcription words themselves."""
        document = {"audio_clip_id" : audio_clip_id,
                    "transcription" : words,
                    "level" : level}
        self.upsert_artifact("reference_transcriptions",document,document)
        return self.get_artifact("reference_transcriptions",document,field="_id")
    
    def create_worker_artifact(self,worker_id):
        document = {"_id": worker_id,
                    "approved_transcription_ids":[],
                    "denied_transcription_ids":[],
                    "submitted_assignments":[],
                    "prequalification_assignment_id":None,
                    "primary_author_merged_transcription_ids":[],
                    "state":"New"}
        self.upsert_artifact("workers",{"_id": worker_id},document)
        return self.get_artifact("workers",document,field="_id")
    
    def create_audio_clip_artifact(self,source_id,source_start_time,source_end_time,
                                   uri,http_url,length_seconds,
                                   disk_space,reference_transcription_id=None,
                                   state="Sourced"):
        """A -1 endtime means to the end of the clip."""
        document = {"source_id" : source_id,
                    "source_start_time" :source_start_time,
                    "source_end_time" : source_end_time,
                    "uri" : uri,
                    "http_url": http_url,
                    "length_seconds" : length_seconds,
                    "disk_space" : disk_space,
                    "reference_transcription_id" : reference_transcription_id,
                    "state" : state}
        self.upsert_artifact("audio_clips", document, document)
        return self.get_artifact("audio_clips",document,field="_id")
    
    def update_transcription_state(self,transcription,state):
        """Use secondary ID audio clip + assignment"""
        search = {"audio_clip_id": transcription["audio_clip_id"],
                    "assignment_id": transcription["assignment_id"]}
        document = {"assignment_id": transcription["assignment_id"],
                    "audio_clip_id": transcription["audio_clip_id"],
                    "transcription": transcription["transcription"],
                    "worker_id": transcription["worker_id"],
                    "state" : state}
        self.upsert_artifact("transcriptions", search, document)
        self.logger.info("Updated transcription w/ audio clip(%s) in assignment(%s) for worker (%s)"\
                         %(transcription["audio_clip_id"],transcription["assignment_id"],\
                           transcription["worker_id"]))
        
    def add_assignment_to_worker(self,worker_id,assignment_tup):
        self.c["workers"].update({"_id":worker_id},{"$addToSet":{"submitted_assignments": assignment_tup}})            
                                            
    def update_audio_source_audio_clip(self,source_id,clip_id):
        """For the audio source given the id, set
            the value using the document"""
        self.c["audio_sources"].update({"_id":source_id},{"$set": {"audio_clip_id": clip_id}})
        self.c["audio_sources"].update({"_id":source_id},{"$set": {"state" : "Clipped"}})
        
    def update_audio_clip_reference_transcription(self,clip_id,reference_transcription_id):
        self.c["audio_clips"].update({"_id":clip_id},
                                {"$set" : {"reference_transcription_id" : reference_transcription_id}})
        self.c["audio_clips"].update({"_id":clip_id},
                                {"$set" : {"state" : "Referenced"}})
        
    def update_assignment_state(self,assignment,state):
        self.c["assignments"].update({"_id":assignment["_id"]},
                                {"$set":{"state":state}})       
        
    def update_transcription_hit_state(self,hit_id,new_state):
        self.c["transcription_hits"].update({"_id":hit_id},  {"$set" : {"state" : new_state}}  )   
        self.logger.info("Updated transcription hit(%s) state to: %s"%(hit_id,new_state))
        return True
    
    def get_all_workers(self):
        return self.c["workers"].find({})
    
    def get_worker_assignments(self,worker):
        """Returns approved and denied assignments submitted by the worker."""
        approved = []
        denied = []
        for assignment in worker["submitted_assignments"]:
            assignment, average_wer = assignment
            a = self.get_artifact("assignments",{"_id":assignment,"state":"Approved"},"_id")
            d = self.get_artifact("assignments",{"_id":assignment,"state":"Denied"},"_id")
            if a: approved.append(a)
            elif d: denied.append(d)
        return approved, denied    
    
    def get_worker_assignments_wer(self,worker):
        """Returns the approved and denied assignments and their average WER"""
        approved = []
        denied = []
        for assignment in worker["submitted_assignments"]:
            assignment, average_wer = assignment
            a = self.get_assignment({"_id":assignment,"state":"Approved"},"_id")
            d = self.get_assignment({"_id":assignment,"state":"Denied"},"_id")
            if a: approved.append((a,average_wer))
            elif d: denied.append((d,average_wer))
        return approved, denied 
    
    def remove_transcription_hit(self,hit_id):
        self.remove_artifacts_by_id("transcription_hits", [hit_id])
                
    def get_audio_clip_pairs(self,clip_queue):
        return [(self.get_artifact("audio_clips",{"_id":w["audio_clip_id"]},field="http_url"),w["audio_clip_id"]) for w in clip_queue]
        #return [(self.get_audio_clip_url(w["audio_clip_id"]),w["audio_clip_id"]) for w in clip_queue]
    
    def get_audio_clips(self,field,ids):
        return self.get_artifacts("audio_clips",field,ids)
    
    def get_transcription_pairs(self,assignment_id):
        """Given an assignment_id, return the hypothesis and 
            reference transcriptions, if they exist"""
        assignment = self.get_artifact("assignments",{"_id":assignment_id})
        pairs = []
        for transcription_id in assignment["transcriptions"]:
            transcription = self.get_artifact("transcriptions",{"_id":transcription_id})
            reference_id = self.get_artifact("audio_clips", {"_id": transcription["audio_clip_id"]}, "reference_transcription_id")
            #reference_id = self.get_audio_clip_by_id(transcription["audio_clip_id"],"reference_transcription_id")
            if reference_id:
                reference_transcription = self.get_artifact("reference_transcriptions",{"_id": reference_id},"transcription")
                #reference_transcription = self.get_reference_transcription({"_id":reference_id},"transcription")            
                pairs.append((reference_transcription,transcription["transcription"]))
        return pairs
            
    def queue_clip(self,audio_clip_id,priority=1,max_queue_size=3):
        """Queue the audio clip."""        
        self.c["audio_clip_queue"].update({"audio_clip_id": audio_clip_id},
                             {"audio_clip_id": audio_clip_id,
                              "priority": priority,
                              "max_size": max_queue_size,
                              "processing" : None,
                              },
                             upsert = True)
        self.update_artifacts_by_id("audio_clips",[audio_clip_id],"state","Queued")
        self.logger.info("Queued clip: %s "%audio_clip_id)
        
    def get_audio_clip_queue(self):
        """Insert the audio clip by id into the queue.
            Get all the clips waiting in the queue and not being processed
            Find the largest queue that is full
            Update the queue and return the clips"""            
        self.revive_audio_clip_queue()
        queue = self.c["audio_clip_queue"].find({"processing":None})
        max_sizes = defaultdict(list)
        for clip in queue:
            #For all the clips
            id = clip["_id"]
            clip_id = clip["audio_clip_id"]
            priority = clip["priority"]
            processing = clip["processing"]
            max_size = int(clip["max_size"])
            max_sizes[max_size].append(clip)
        #Get the largest full queue
        max_q = self.get_max_queue(max_sizes)        
        for clip in max_q:
            t = time()
            self.c["audio_clip_queue"].find_and_modify(query = {"_id":clip["_id"]},
                                                 update = { "$set" : {"processing":t}}
                                                )
        return max_q
    
    def initialize_test_db(self):
        audio_clips = [{ "_id" : "12345", "audio_clip_url" : "http://www.cis.upenn.edu/~tturpen/wavs/testwav.wav", "reference" : None, "state" : "New" },
                       { "_id" : "54321", "audio_clip_url" : "http://www.cis.upenn.edu/~tturpen/wavs/testwav.wav", "reference" : None, "state" : "New" },
                       { "_id" : "98765", "audio_clip_url" : "http://www.cis.upenn.edu/~tturpen/wavs/testwav.wav", "reference" : None, "state" : "New" }]
        audio_clip_queue = [{"audio_clip_id" : "12345", "priority" : 1, "processing" : None, "max_size" : 3 },
                            {"audio_clip_id" : "54321", "priority" : 1, "processing" : None, "max_size" : 3 },
                            {"audio_clip_id" : "98765", "priority" : 1, "processing" : None, "max_size" : 3 }]
        transcriptions = [{ "assignment_id" : "21B85OZIZEHRPTEQZZH5HWPXNKOBZK", "audio_clip_id" : "12345", "worker_id" : "A2WBBX5KW5W6GY", "state" : "Submitted", "transcription" : "This is the first transcription." },
                          { "assignment_id" : "21B85OZIZEHRPTEQZZH5HWPXNKOBZK", "audio_clip_id" : "12345.0", "worker_id" : "A2WBBX5KW5W6GY", "state" : "Submitted", "transcription" : "This is the second transcription.|This is the third transcription." }]
    
def main():
    mh = MongoHandler()
    t = Turker(5)
    print(t.get_mongo_entry())
    mh.insert_entity(t)
    
if __name__ == "__main__":
    main()
        
       
        
        
    