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
from handlers.Exceptions import MultipleResultsOnIdFind, TooManyEntries, DuplicateArtifactException, WrongParametersExecption
from time import time
from collections import defaultdict
from bson.objectid import ObjectId
from statemaps import Elicitation
from statemaps.Elicitation import Prompt, PromptSource, ElicitationHit, RecordingSource, ElicitationAssignment, Worker
from handlers.Recording import RecordingHandler

import logging
MAX_QUEUE_VIEW = 100

class MongoTranscriptionHandler(object):
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
        self.c["assignments"] = self.db.assignments
        self.c["workers"] = self.db.workers
        
        self.logger = logging.getLogger("transcription_engine.mongodb_transcription_handler")
        
    def get_artifact_by_id(self,collection,art_id,field=None,refine={}):
        return self.get_artifact(collection,{"_id":ObjectId(art_id)},field,refine)
    
    def get_artifacts_by_state(self,collection,state,field=None,refine={}):
        return self.c[collection].find({"state":state})
    
    def get_artifacts(self,collection,param,values,field=None,refine={}):
        return [self.get_artifact(collection,{param:val},field,refine) for val in values]
    
    def get_all_artifacts(self,collection):
        return self.c[collection].find()
    
    def get_artifact(self,collection,search,field=None,refine={}):
        """Artifacts are the atomic data type for this database.
            Each artifact belongs to a collection in the database.""" 
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
    
    def clips_already_in_hit(self,clip_pairs):
        clips_in_hits = []
        for pair in clip_pairs:
            for hit in self.get_all_artifacts("transcription_hits"):
                if pair in hit["clips"]:
                    clips_in_hits.append(pair[1])#Append the clip id
                    self.logger.info("Audio clip(%s) already in HIT(%s)"(pair[1],hit["_id"]))
        return clips_in_hits
    
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
        
    def create_artifact(self,collection,search,document):
        art_id = self.get_artifact(collection, search, "_id")
        if not art_id:
            self.c[collection].insert(document)
            art_id = self.get_artifact(collection, document,"_id")
            self.logger.info("Created %s artifact(%s) "%(collection,art_id))
        return art_id       
        
    def create_transcription_hit_artifact(self,hit_id,hit_type_id,clip_queue,new_state):
        if type(clip_queue) != list:
            raise IOError
        clips = [w["audio_clip_id"] for w in clip_queue]
        document =  {"_id":hit_id,
                     "hit_type_id": hit_type_id,
                     "clips" : clips,
                     "state": new_state}
        art_id = self.create_artifact("transcription_hits",{"_id": hit_id},document)
        return art_id
    
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
        art_id = self.create_artifact("assignments", {"_id": assignment_id},document)        
        return art_id
    
    def create_audio_source_artifact(self,uri,disk_space,length_seconds,
                                     sample_rate,speaker_id,
                                     encoding,audio_clip_id=None,state="New"):
        """Each audio source is automatically an audio clip,
            Therefore the reference transcription can be found
            by referencing the audio clip assigned to this source.
            For turkers, speaker id will be their worker id"""    
        search = {"uri" : uri,
                    "disk_space" : disk_space,
                    "length_seconds" : length_seconds,
                    "sample_rate" : sample_rate,
                    "speaker_id" : speaker_id,
                    "encoding" : encoding}    
        document = {"uri" : uri,
                    "disk_space" : disk_space,
                    "length_seconds" : length_seconds,
                    "audio_clip_id" : audio_clip_id,
                    "sample_rate" : sample_rate,
                    "speaker_id" : speaker_id,
                    "encoding" : encoding,
                    "state" : state}
        #soft source checking
        art_id = self.create_artifact("audio_sources", search, document)
        return art_id
        
    def create_reference_transcription_artifact(self,audio_clip_id,words,level):
        """Create the reference transcription given the audio clip id
            and the transcription words themselves."""
        document = {"audio_clip_id" : audio_clip_id,
                    "transcription" : words,
                    "level" : level}
        art_id = self.create_artifact("reference_transcriptions",document,document)
        return art_id
    
    def create_worker_artifact(self,worker_id):
        document = {"_id": worker_id,
                    "approved_transcription_ids":[],
                    "denied_transcription_ids":[],
                    "submitted_assignments":[],
                    "prequalification_assignment_id":None,
                    "primary_author_merged_transcription_ids":[],
                    "state":"New"}
        art_id = self.create_artifact("workers",{"_id": worker_id},document)
        return art_id
    
    def create_audio_clip_artifact(self,source_id,source_start_time,source_end_time,
                                   uri,http_url,length_seconds,
                                   disk_space,reference_transcription_id=None,
                                   state="Sourced"):
        """A -1 endtime means to the end of the clip."""
        search = {"source_id" : source_id,
                    "source_start_time" :source_start_time,
                    "source_end_time" : source_end_time,
                    "uri" : uri,
                    "http_url": http_url,
                    "length_seconds" : length_seconds,
                    "disk_space" : disk_space}
        document = {"source_id" : source_id,
                    "source_start_time" :source_start_time,
                    "source_end_time" : source_end_time,
                    "uri" : uri,
                    "http_url": http_url,
                    "length_seconds" : length_seconds,
                    "disk_space" : disk_space,
                    "reference_transcription_id" : reference_transcription_id,
                    "state" : state}
        art_id = self.create_artifact("audio_clips", search, document)
        return art_id
    
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
            a = self.get_artifact("assignments",{"_id":assignment,"state":"Approved"},"_id")
            d = self.get_artifact("assignments",{"_id":assignment,"state":"Denied"},"_id")
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
    
    def update_artifact_by_id(self,collection,artifact_id,field=None,value=None,document=None):
        if document:
            #Careful, this replaces the document entirely
            if ObjectId.is_valid(artifact_id):
                self.c[collection].update({"_id":ObjectId(artifact_id)}, document)
            else:
                self.c[collection].update({"_id":artifact_id}, document)
        elif field and value:
            if ObjectId.is_valid(artifact_id):
                self.c[collection].update({"_id":ObjectId(artifact_id)}, {"$set" : {field:value}})
            else:
                self.c[collection].update({"_id":artifact_id}, {"$set" : {field:value}})
        else:
            raise WrongParametersExecption
    
class MongoElicitationHandler(object):
    default_db_loc = 'mongodb://localhost:27017'

    def __init__(self):
        self.queue_revive_time = 5 
        client = MongoClient(self.default_db_loc)
        self.db_name = "elicitation"
        self.db = client.production_elicitation_db
        self.c = {}#dictionary of collections
        
        #Initialize the state maps
        self.c["state_maps"] = self.db.state_maps
        self.initialize_state_map()        

        self.rh = RecordingHandler()
        self.c["recording_sources"] = self.db.recording_sources
        self.c["prompt_sources"] = self.db.prompt_sources
        self.c["prompts"] = self.db.prompts
        self.c["hits"] = self.db.prompt_hits
        self.c["prompt_queue"] = self.db.prompt_queue
        self.c["elicitation_assignments"] = self.db.elicitation_assignments
        self.c["elicitation_hits"] = self.db.elicitation_hits
        self.c["workers"] = self.db.workers 
        self.logger = logging.getLogger("transcription_engine.mongodb_elicitation_handler")
        
    def initialize_state_map(self):
        prompt_map = {"prompt_sources": ["PromptSource",PromptSource().map],
                      "prompts": ["Prompt",Prompt().map],
                      "elicitation_hits": ["ElicitationHit",ElicitationHit().map],
                      "recording_sources" : ["RecordingSource", RecordingSource().map],
                      "elicitation_assignments" : ["ElicitationAssignment",ElicitationAssignment().map],
                      "workers" : ["Worker", Worker().map]
                      }
        self.c["state_maps"].remove({})
        self.c["state_maps"].insert(prompt_map)        
        
    def get_artifact_by_id(self,collection,art_id,field=None,refine={}):
        if ObjectId.is_valid(art_id):
            return self.get_artifact(collection,{"_id":ObjectId(art_id)},field,refine)
        return self.get_artifact(collection,{"_id":art_id},field,refine)
    
    def get_artifacts_by_state(self,collection,state,field=None,refine={}):
        return self.c[collection].find({"state":state})
    
    def get_artifacts(self,collection,param,values,field=None,refine={}):
        return [self.get_artifact(collection,{param:val},field,refine) for val in values]
    
    def get_all_artifacts(self,collection):
        return self.c[collection].find()
    
    def get_artifact(self,collection,search,field=None,refine={}):
        """Artifacts are the atomic data type for this database.
            Each artifact belongs to a collection in the database.""" 
        responses = self.c[collection].find(search,refine)\
                    if refine else self.c[collection].find(search)
        prev = False
        for response in responses:
            if prev:
                raise TooManyEntries("MongoElicitationHandler.get_artifact."+collection)
            prev = response        
        return field in prev and prev[field] if field and prev else prev
    
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
    
    def prompts_already_in_hit(self,prompt_pairs):
        prompts_in_hits = []
        for pair in prompt_pairs:
            for hit in self.get_all_artifacts("elicitation_hits"):
                if pair in hit["prompts"]:
                    prompts_in_hits.append(pair[1])#Append the clip id
                    self.logger.info("Audio clip(%s) already in HIT(%s)"(pair[1],hit["_id"]))
        return prompts_in_hits
    
    def revive_queue(self,queue_name):
        """If an audio clip in the queue has been processing for more than
            queue_revive_time seconds, free the clip by resetting processing"""
        non_none = self.c[queue_name].find({"processing": {"$ne" : None}})
        for artifact in non_none:
            if  time() - artifact["processing"] > self.queue_revive_time:
                self.c[queue_name].update({"_id":artifact["_id"]}, {"$set" : {"processing" : None}})  
        self.logger.info("Finished reviving database(%s) queue."%self.db_name)
        
    def init_artifact_set(self,collection,artifact_id,field,values):
        if not self.get_artifact_by_id(collection, artifact_id, field):
            if ObjectId.is_valid(artifact_id):
                self.c[collection].update({"_id":ObjectId(artifact_id)},{"$set":{field:values}})
            else:
                self.c[collection].update({"_id":artifact_id},{"$set":{field:values}})
    
    def remove_artifacts(self,collection,artifacts):
        """Given a list of artifacts, remove each one by _id"""
        for artifact in artifacts:
            self.c[collection].remove({"_id":artifact["_id"]})
    
    def remove_artifacts_by_id(self,collection,artifact_ids):
        """Given a list of artifacts, remove each one by _id"""
        for artifact_id in artifact_ids:
            self.c[collection].remove({"_id":artifact_id})
         
    def update_artifact_with_document(self,collection,artifact_id,document):   
        """Given a list of artifact ids, update each one's field to value"""
        self.c[collection].update({"_id":artifact_id}, document)
                    
    def update_artifact_by_id(self,collection,artifact_id,field=None,value=None,document=None):
        if document:
            #Careful, this replaces the document entirely
            if ObjectId.is_valid(artifact_id):
                self.c[collection].update({"_id":ObjectId(artifact_id)}, document)
            else:
                self.c[collection].update({"_id":artifact_id}, document)
            if field != "state":
                #Because updating the state calls this method
                self.update_artifact_state(collection, artifact_id)
        elif field and value:
            if ObjectId.is_valid(artifact_id):
                self.c[collection].update({"_id":ObjectId(artifact_id)}, {"$set" : {field:value}})
            else:
                self.c[collection].update({"_id":artifact_id}, {"$set" : {field:value}})
            if field != "state":
                #Because updating the state calls this method
                self.update_artifact_state(collection, artifact_id)          
        else:
            raise WrongParametersExecption
                
    def update_artifacts_by_id(self,collection,artifact_ids,field,value,document=None):
        """Given a list of artifact ids, update each one's field to value"""
        if type(artifact_ids) != list:
            self.logger.error("Error updating %s audio clip(%s)"%(collection,artifact_ids))
            raise IOError
        for artifact_id in artifact_ids:
            self.update_artifact_by_id(collection,artifact_id,field,value,document)

              
    
    def upsert_artifact(self,collection,search,document):
        """Artifacts that have external imposed unique IDs
            can be created with an upsert."""            
        self.c[collection].update(search,document,upsert = True)        
            
    def remove_artifacts_from_queue(self,queue_name,artifact_queue):
        """Remove the artifact_queue from the queue"""
        self.remove_artifacts(queue_name,artifact_queue)
        self.logger.info("Finished updating database(%s) queue."%self.db_name) 
        
    def get_collection_state_map(self,collection):
        return self.c["state_maps"].find({})[0][collection]
            
    def induce_artifact_state(self,collection,artifact_id):
        func_class, states = self.get_collection_state_map(collection)
        instance = getattr(Elicitation,func_class)()
        artifact = self.get_artifact_by_id(collection, artifact_id)
        prev_state = "New"
        for state in states:
            func = getattr(instance,state)
            res = func(artifact)
            if not res:
                return prev_state
            prev_state = state
        return state
    
    def update_artifacts_state(self,collection,artifact_ids):
        for artifact_id in artifact_ids:
            self.update_artifact_state(collection, artifact_id)
        else:
            return True
        return False
    
    def update_artifact_state(self,collection,artifact_id):
        """For ease of use, each class has an explicit state attribute value.
            However, states can always be determined by the state_map"""
        new_state = self.induce_artifact_state(collection,artifact_id)
        self.update_artifacts_by_id(collection,[artifact_id],"state",new_state)
        self.logger.info("Updated(%s) state for: %s to %s"%(collection,artifact_id,new_state))
        return True
    
    def create_artifact(self,collection,search,document,update=True):
        """Check to see if the artifact exists given search,
            create the artifact
            update the state by induction."""
        art_id = self.get_artifact(collection, search, "_id")
        if not art_id:
            self.c[collection].insert(document)
            art_id = self.get_artifact(collection, document,"_id")
            self.logger.info("Created %s artifact(%s) "%(collection,art_id))
        elif update:
            self.update_artifact_by_id(collection,art_id,document=document)
        self.update_artifact_state(collection, art_id)
        return art_id       
        
    def create_elicitation_hit_artifact(self,hit_id,hit_type_id,prompt_ids):
        if type(prompt_ids) != list:
            raise IOError
        document =  {"_id":hit_id,
                     "hit_type_id": hit_type_id,
                     "prompts" : prompt_ids}
        art_id = self.create_artifact("elicitation_hits",{"_id": hit_id},document)
        return art_id
    
    def create_assignment_artifact(self,assignment,answers):
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
                     "recordings" : answers}
        art_id = self.create_artifact("elicitation_assignments", {"_id": assignment_id},document)        
        return art_id
    
    def create_prompt_source_artifact(self,uri,disk_space,prompt_count):
        """Each audio source is automatically an audio clip,
            Therefore the reference transcription can be found
            by referencing the audio clip assigned to this source.
            For turkers, speaker id will be their worker id"""    
        search = {"uri" : uri,
                    "disk_space" : disk_space,
                    "prompt_count": prompt_count}    
        document = {"uri" : uri,
                    "disk_space" : disk_space,
                    "prompt_count": prompt_count} 
        #soft source checking
        art_id = self.create_artifact("prompt_sources", search, document)
        return art_id
        
    def create_worker_artifact(self,worker_id):
        document = {"eid": worker_id}
        art_id = self.create_artifact("workers",{"eid": worker_id},document)
        return art_id
    
    def create_prompt_artifact(self,source_id, words, normalized_words,line_number,rm_prompt_id,word_count):
        """A -1 endtime means to the end of the clip."""
        search = {"source_id" : source_id,
                    "line_number" : line_number,
                    "rm_prompt_id" : rm_prompt_id,
                    "word_count": word_count}
        document = {"source_id" : source_id,
                    "line_number" : line_number,
                    "rm_prompt_id" : rm_prompt_id,
                    "words" : words,
                    "normalized_words": normalized_words,
                    "word_count": word_count}
        art_id = self.create_artifact("prompts", search, document)
        return art_id
    
    def create_recording_source_artifact(self,prompt_id,recording_url,worker_id):
        """Use the recording handler to download the recording
            and create the artifact"""
        recording_uri = self.rh.download_vocaroo_recording(recording_url)
        search = {"recording_url" : recording_url}
        document = {"recording_url": recording_url,
                    "prompt_id": prompt_id,
                    "recording_uri": recording_uri,
                    "worker_id": worker_id}
        art_id = self.create_artifact("recording_sources", search, document)
        return art_id
        
    def add_item_to_artifact_set(self,collection,artifact_id,field,value):
        self.init_artifact_set(collection,artifact_id,field,[])
        if ObjectId.is_valid(artifact_id):
            self.c[collection].update({"_id":ObjectId(artifact_id)},{"$addToSet":{field: value}})
        else:
            self.c[collection].update({"_id":artifact_id},{"$addToSet":{field: value}})
        self.update_artifact_state(collection, artifact_id)
        
    def add_assignment_to_worker(self,worker_id,assignment_tup):
        self.c["workers"].update({"_id":worker_id},{"$addToSet":{"submitted_assignments": assignment_tup}})            
                                            
    def update_assignment_state(self,assignment,state):
        self.c["assignments"].update({"_id":assignment["_id"]},
                                {"$set":{"state":state}})       
        
    def update_elicitation_hit_state(self,hit_id):
        return self.update_artifact_state("elicitation_hits", hit_id)        
    
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
    
    def remove_elicitation_hit(self,hit_id):
        self.remove_artifacts_by_id("elicitation_hits", [hit_id])
                
    def get_prompt_pairs(self,prompt_queue):
        """Given the queue entries, return the id and text for the prompt
        """
        return [(self.get_artifact("prompts",{"_id":w["prompt_id"]},field="normalized_words"),w["prompt_id"]) for w in prompt_queue]
               
    def enqueue_prompt(self,prompt_id,priority=1,max_queue_size=3):
        """Queue the audio prompt."""        
        self.c["prompt_queue"].update({"prompt_id": prompt_id},
                             {"prompt_id": prompt_id,
                              "priority": priority,
                              "max_size": max_queue_size,
                              "processing" : None,
                              },
                             upsert = True)
        self.update_artifacts_by_id("prompts", [prompt_id], "inqueue", "prompt_queue")
        self.logger.info("Queued prompt: %s "%prompt_id)
        
    def get_prompt_queue(self):
        """Insert the audio clip by id into the queue.
            Get all the clips waiting in the queue and not being processed
            Find the largest queue that is full
            Update the queue and return the clips"""
        qname = "prompt_queue"          
        self.revive_queue("prompt_queue")
        queue = self.c[qname].find({"processing":None}).limit(MAX_QUEUE_VIEW)
        max_sizes = defaultdict(list)
        for prompt in queue:
            #For all the prompts
            priority = prompt["priority"]
            processing = prompt["processing"]
            max_size = int(prompt["max_size"])
            max_sizes[max_size].append(prompt)
        #Get the largest full queue
        max_q = self.get_max_queue(max_sizes)        
        for clip in max_q:
            t = time()
            self.c[qname].find_and_modify(query = {"_id":clip["_id"]},
                                                 update = { "$set" : {"processing":t}})
        return max_q

def main():
    mh = MongoTranscriptionHandler()
    t = Turker(5)
    print(t.get_mongo_entry())
    mh.insert_entity(t)
    
if __name__ == "__main__":
    main()
        
       
        
        
    