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
from boto.mturk.connection import MTurkConnection, MTurkRequestError, ResultSet
from handlers.MechanicalTurk import AssignmentHandler, TurkerHandler, HitHandler
from handlers.MongoDB import MongoElicitationHandler

from handlers.Audio import WavHandler, PromptHandler
from handlers.Exceptions import WavHandlerException, PromptNotFound
from filtering.StandardFilter import Filter
from util.calc import cer_wer
from shutil import copyfile

from text.Normalization import Normalize

import logging
import os
import datetime
import time

HOST='mechanicalturk.amazonaws.com'
#HOST='mechanicalturk.sandbox.amazonaws.com'
TEMPLATE_DIR = "/home/taylor/csaesr/src/resources/resources.templates/"
cost_sensitive = True

class ElicitationPipelineHandler(object):

    def __init__(self):
        aws_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_k = os.environ['AWS_ACCESS_KEY']

        try:
            self.conn = MTurkConnection(aws_access_key_id=aws_id,\
                          aws_secret_access_key=aws_k,\
                          host=HOST)
        except Exception as e:
            print(e)        
        
        self.ah = AssignmentHandler(self.conn)
        self.th = TurkerHandler(self.conn)
        self.hh = HitHandler(self.conn,TEMPLATE_DIR)
        self.mh = MongoElicitationHandler()
        self.ph = PromptHandler()
        self.filter = Filter(self.mh)
        self.balance = self.conn.get_account_balance()[0].amount
        self.batch_cost = 20
        if self.balance > self.batch_cost:
            self.balance = self.batch_cost
        else:
            raise IOError
        self.logger = logging.getLogger("transcription_engine.elicitation_pipeline_handler")
        
    def load_PromptSource_RawToList(self,prompt_file_uri):
        """Create the prompt artifacts from the source."""        
        prompt_dict = self.ph.get_prompts(prompt_file_uri)        
        disk_space = os.stat(prompt_file_uri).st_size
        source_id = self.mh.create_prompt_source_artifact(prompt_file_uri, disk_space, len(prompt_dict))
        normalizer = Normalize()
        for key in prompt_dict:
            prompt, line_number = prompt_dict[key]
            normalized_prompt =  normalizer.rm_prompt_normalization(prompt)
            self.mh.create_prompt_artifact(source_id, prompt, normalized_prompt, line_number, key, len(prompt))       
            
    def load_assignment_hit_to_submitted(self):
        """Check all assignments for audio clip IDs.
            Update the audio clips.
            This is a non-destructive load of the assignments from MTurk"""
        hits = self.conn.get_all_hits()
        for hit in hits:
            transcription_dicts = [{}]
            hit_id = hit.HITId
            if self.mh.get_artifact("elicitation_hits",{"_id": hit_id}):
                assignments = self.conn.get_assignments(hit_id)
                have_all_assignments = True
                assignment_ids = []
                for assignment in assignments:
                    assignment_id = assignment.AssignmentId
                    assignment_ids.append(assignment_id)  
                    if self.mh.get_artifact("elicitation_assignments",{"_id":assignment.AssignmentId}):
                        #We create assignments here, so if we already have it, skip
                        #continue
                        pass
                    else:
                        have_all_assignments = False                                         
                    recording_ids = []                
                    prompt_id_tag = "prompt_id"
                    recording_url_tag = "recording_url"
                    worker_id_tag = "worker_id"
                    recording_dict = self.ah.get_assignment_submitted_text_dict(assignment,prompt_id_tag,recording_url_tag)
                    worker_oid = self.mh.create_worker_artifact(assignment.WorkerId)   
                    for recording in recording_dict:
                        if not self.mh.get_artifact_by_id("prompts",recording[prompt_id_tag]): 
                            self.logger.info("Assignment(%s) with unknown %s(%s) skipped"%\
                                        (assignment_id,prompt_id_tag,recording[prompt_id_tag]))
                            break                        
                        recording_id = self.mh.create_recording_source_artifact(recording[prompt_id_tag],
                                                                         recording[recording_url_tag],
                                                                         recording[worker_id_tag])
                        self.mh.add_item_to_artifact_set("prompts", recording[prompt_id_tag], "recording_sources",
                                                       recording_id)
                        recording_ids.append(recording_id)
                    else:
                        self.mh.create_assignment_artifact(assignment,
                                                       recording_ids)
                        self.mh.add_item_to_artifact_set("elicitation_hits", hit_id, "submitted_assignments", assignment_id)
                        self.mh.add_item_to_artifact_set("workers", worker_oid, "submitted_assignments", assignment_id)
                print("Elicitation HIT(%s) submitted assignments: %s "%(hit_id,assignment_ids))    

    def approve_assignment_submitted_to_approved(self):
        """Approve all submitted assignments"""
        hits = self.conn.get_all_hits()
        for hit in hits:
            transcription_dicts = [{}]
            hit_id = hit.HITId
            if self.mh.get_artifact("elicitation_hits",{"_id": hit_id}):
                assignments = self.conn.get_assignments(hit_id)
                have_all_assignments = True
                assignment_ids = []
                for assignment in assignments:
                    assignment_id = assignment.AssignmentId
                    assignment_ids.append(assignment_id)  
                    if self.mh.get_artifact("elicitation_assignments",{"_id":assignment.AssignmentId,"state":"Submitted"}):
                        #WARNING: this Approves every assignment
                        self.conn.approve_assignment(assignment_id, "Thank you for completing this assignment!")
                        self.mh.update_artifact_by_id("elicitation_assignments", assignment_id, "approval_time", datetime.datetime.now())                        
                        
    def get_assignment_stats(self):
        effective_hourly_wage = self.effective_hourly_wage_for_approved_assignments(.25)                    
    
    def effective_hourly_wage_for_approved_assignments(self,reward_per_assignment):
        """Calculate the effective hourly wage for Approved Assignments"""        
        approved_assignments = self.mh.get_artifacts_by_state("elicitation_assignments","Approved")
        total = datetime.timedelta(0)
        count = 0
        for assignment in approved_assignments:
            accepted = datetime.datetime.strptime(assignment["AcceptTime"],"%Y-%m-%dT%H:%M:%SZ")
            submitted = datetime.datetime.strptime(assignment["SubmitTime"],"%Y-%m-%dT%H:%M:%SZ")
            total += submitted-accepted
            count += 1
            #self.mh.update_artifact_by_id("elicitation_assignments", assignment["_id"], "SubmitTime", completion_time)
        seconds_per_assignment = total.total_seconds()/count
        effective_hourly_wage = 60.0*60.0/seconds_per_assignment * reward_per_assignment
        print("Effective completion time(%s) *reward(%s) = %s"%(seconds_per_assignment,reward_per_assignment,effective_hourly_wage))
                        
    def recording_sources_generate_worker_sorted_html(self):
        sources = self.mh.get_all_artifacts("recording_sources")
        for source in sources:
            if not self.mh.get_artifact("workers", {"eid": source["worker_id"]}):
                pass
            
    def enqueue_prompts_and_generate_hits(self):
        prompts = self.mh.get_artifacts_by_state("prompts", "New")
        for prompt in prompts:
            self.mh.enqueue_prompt(prompt["_id"], 1, 5)
            prompt_queue = self.mh.get_prompt_queue()
            prompt_pairs = self.mh.get_prompt_pairs(prompt_queue)
            if prompt_pairs:
                hit_title = "Audio Elicitation"
                question_title = "Speak and Record your Voice" 
                keywords = "audio, elicitation, speech, recording"
                if cost_sensitive:
                    reward_per_clip = 0.05
                    max_assignments = 2
                    estimated_cost = self.hh.estimate_html_HIT_cost(prompt_pairs,reward_per_clip=reward_per_clip,\
                                                                    max_assignments=max_assignments)
                    prompts_in_hits = self.mh.prompts_already_in_hit(prompt_pairs)
                    if prompts_in_hits:
                        #If one or more clips are already in a HIT, remove it from the queue
                        self.mh.remove_artifact_from_queue(prompts_in_hits)
                    elif self.balance - estimated_cost >= 0:
                        #if we have enough money, create the HIT
                        response = self.hh.make_html_elicitation_HIT(prompt_pairs,hit_title,
                                                     question_title, keywords,max_assignments=max_assignments,reward_per_clip=reward_per_clip)
#                         response = self.hh.make_question_form_elicitation_HIT(prompt_pairs,hit_title,
#                                                      question_title, keywords)
                        self.balance = self.balance - estimated_cost
                        if type(response) == ResultSet and len(response) == 1 and response[0].IsValid:
                            response = response[0]
                            self.mh.remove_artifacts_from_queue("prompt_queue",prompt_queue)
                            prompt_ids = [w["prompt_id"] for w in prompt_queue]    
                            hit_id = response.HITId
                            hit_type_id = response.HITTypeId
                            self.mh.create_elicitation_hit_artifact(hit_id,hit_type_id,prompt_ids)  
                            self.mh.update_artifacts_by_id("prompts", prompt_ids, "hit_id", hit_id)      
                            self.logger.info("Successfully created HIT: %s"%hit_id)
                    else:
                        return True
                    
    def allhits_liveness(self):
        #allassignments = self.conn.get_assignments(hit_id)
        #first = self.ah.get_submitted_transcriptions(hit_id,str(clipid))

        hits = self.conn.get_all_hits()
        selection = raw_input("Remove all hits with no assignments?")
        if selection == "y":
            for hit in hits:
                hit_id = hit.HITId
                assignments = self.conn.get_assignments(hit_id)
                if len(assignments) == 0:
                    try:
                        self.conn.disable_hit(hit_id)
                        prompts = self.mh.get_artifact("elicitation_hits",{"_id": hit_id},"prompts")
                        self.mh.remove_elicitation_hit(hit_id)
                        if prompts:
                            self.mh.update_artifacts_state("prompts", prompts)
                        else:
                            pass
                    except MTurkRequestError as e:
                        raise e
            return True
        for hit in hits:
            hit_id = hit.HITId            
            print("HIT ID: %s"%hit_id)
            assignments = self.conn.get_assignments(hit_id)
            if len(assignments) == 0:
                if raw_input("Remove hit with no submitted assignments?(y/n)") == "y":
                    try:
                        self.conn.disable_hit(hit_id)
                        prompts = self.mh.get_artifact("elicitation_hits",{"_id": hit_id},"prompts")
                        self.mh.remove_elicitation_hit(hit_id)
                        if prompts:
                            self.mh.update_artifacts_state("prompts", prompts)
                        else:
                            pass
                    except MTurkRequestError as e:
                        raise e
            else:
                if raw_input("Remove hit with %s submitted assignments?(y/n)"%len(assignments)) == "y":
                    try:
                        self.conn.disable_hit(hit_id)
                    except MTurkRequestError as e:
                        raise e
        
    def run(self):
        #audio_file_dir = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/dep_trn"
        prompt_file_uri = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/doc/al_sents.snr"
        selection = 0
        #self.get_time_submitted_for_assignments()
        while selection != "12":
            selection = raw_input("""Prompt Source raw to Elicitations-Approved Pipeline:\n
                                     1: PromptSource-Load_RawToList: Load Resource Management 1 prompt source files to queueable prompts
                                     2: Prompt-ReferencedToHit: Queue all referenced prompts and create a HIT if the queue is full.
                                     3: Prompt-HitToAssignmentSubmitted: Check all submitted assignments for Elicitations.
                                     4: RecordingSources-GenerateWorkerSortedHtml: Check all submitted assignments for Elicitations.
                                     5: Review Current Hits
                                     6: ElicitationAssignment-SubmittedToApproved: Approve submitted assignments.
                                     7: Review Current Hits
                                     8: Worker liveness
                                     9: Account balance
                                     10: Worker stats
                                     11: Recalculate worker WER
                                     12: Exit
                                    """)
            if selection == "1":
                self.load_PromptSource_RawToList(prompt_file_uri)
            elif selection == "2":
                self.enqueue_prompts_and_generate_hits()
            elif selection == "3":
                self.load_assignment_hit_to_submitted()
            elif selection == "4":
                self.recording_sources_generate_worker_sorted_html()
            elif selection == "5":
                self.allhits_liveness()
            elif selection == "6":
                self.approve_assignment_submitted_to_approved()
            elif selection == "7":
                self.get_assignment_stats()
            else:
                selection = "12"
                
#    prompt_dict = self.ph.get_prompts(prompt_file_uri)

#     def get_time_submitted_for_assignments(self):
#         assignments = self.mh.get_all_artifacts("elicitation_assignments")
#         for assignment in assignments:
#             assignment_id = assignment["_id"]
#             a_assignment = self.conn.get_assignment(assignment_id)[0]
#             self.mh.update_artifact_by_id("elicitation_assignments", assignment_id, "SubmitTime", a_assignment.SubmitTime)