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
from data.Exceptions import NoEntityID

class DataEntity(object):
    def get_mongo_entry(self):
        if not id:
            raise NoEntityID
        return {"_id" : self.id}
        
class Turker(DataEntity):
    def __init__(self,worker_id):
        self.id = worker_id
        
class AudioClip(DataEntity):
    def __init__(self,clip_id):
        self.id = clip_id
        
class AudioSource(DataEntity):
    def __init__(self,clip_id):
        self.id = clip_id

class Transcription(DataEntity):
    def __init__(self,transcript_id):
        self.id = transcript_id
        
class HitEntity(object):
    TYPE_ID = None
    def __init__(self,boto_hit,entry=False):
        if entry:
            self.amount = entry["amount"] 
            self.assignment_duration = entry["assignment_duration"] 
            self.auto_approval_delay = entry["auto_approval_delay"] 
            self.creation_time = entry["creation_time"] 
            self.currency_code = entry["currency_code"] 
            self.description = entry["description"] 
            self.expiration = entry["expiration"] 
            self.formatted_price = entry["formatted_price"] 
            self.hit, entry["hit"] 
            self.hit_group_id = entry["hit_group_id"] 
            self.hit_id = entry["hit_id"] 
            self.hit_review_status = entry["hit_review_status"] 
            self.hit_status = entry["hit_status"] 
            self.hit_type_id = entry["hit_type_id"] 
            self.keywords = entry["keywords"] 
            self.max_assignments = entry["max_assignments"] 
            self.number_of_assignments_available = entry["number_of_assignments_available"] 
            self.number_of_assignments_completed = entry["number_of_assignments_completed"] 
            self.number_of_assignments_pending = entry["number_of_assignments_pending"] 
            self.reward = entry["reward"] 
            self.title = entry["title"] 
            self.expired = entry["expired"]
        else:
            self.amount = boto_hit.Amount
            self.assignment_duration = boto_hit.AssignmentDurationInSeconds
            self.auto_approval_delay = boto_hit.AutoApprovalDelayInSeconds
            self.creation_time = boto_hit.CreationTime
            self.currency_code = boto_hit.CurrencyCode
            self.description = boto_hit.Description
            self.expiration = boto_hit.Expiration
            self.formatted_price = boto_hit.FormattedPrice
            self.hit = boto_hit.HIT
            self.hit_group_id = boto_hit.HITGroupId
            self.hit_id = boto_hit.HITId
            self.hit_review_status = boto_hit.HITReviewStatus
            self.hit_status = boto_hit.HITStatus
            self.hit_type_id = boto_hit.HITTypeId
            self.keywords = boto_hit.Keywords
            self.max_assignments = boto_hit.MaxAssignments
            self.number_of_assignments_available = boto_hit.NumberOfAssignmentsAvailable
            self.number_of_assignments_completed = boto_hit.NumberOfAssignmentsCompleted
            self.number_of_assignments_pending = boto_hit.NumberOfAssignmentsPending
            self.reward = boto_hit.Reward
            self.title = boto_hit.Title
            self.expired = boto_hit.expired
        
    def toString(self):        
        return str("amount: " + self.amount + "\n" +        
        "assignment_duration: " + self.assignment_duration + "\n" +        
        "auto_approval_delay: " + self.auto_approval_delay + "\n" +
        "creation_time: " + self.creation_time + "\n" +
        "currency_code: " + self.currency_code + "\n" +
        "description: " + self.description + "\n" +
        "expiration: " + self.expiration + "\n" +        
        "formatted_price: " + self.formatted_price + "\n" +        
        "hit: " + self.hit + "\n" +
        "hit_group_id: " + self.hit_group_id + "\n" +
        "hit_id: " + self.hit_id + "\n" +
        "hit_review_status: " + self.hit_review_status + "\n" +
        "hit_status: " + self.hit_status + "\n" +
        "hit_type_id: " + self.hit_type_id + "\n" +
        "keywords: " + self.keywords + "\n" +
        "max_assignments: " + self.max_assignments + "\n" +
        "number_of_assignments_available: " + self.number_of_assignments_available + "\n" +
        "number_of_assignments_completed: " + self.number_of_assignments_completed + "\n" +
        "number_of_assignments_pending: " + self.number_of_assignments_pending + "\n" +
        "reward: " + self.reward + "\n" +
        "title: " + self.title + "\n" +
        "expired: " + str(self.expired))
        
    def get_mongo_entry(self):
        return {"amount" : self.amount,
        "assignment_duration" : self.assignment_duration,
        "auto_approval_delay" : self.auto_approval_delay,
        "creation_time" : self.creation_time,
        "currency_code" : self.currency_code,
        "description" : self.description,
        "expiration" : self.expiration,
        "formatted_price" : self.formatted_price,
        "hit" : self.hit,
        "hit_group_id" : self.hit_group_id,
        "hit_id" : self.hit_id,
        "hit_review_status" : self.hit_review_status,
        "hit_status" : self.hit_status,
        "hit_type_id" : self.hit_type_id,
        "keywords" : self.keywords,
        "max_assignments" : self.max_assignments,
        "number_of_assignments_available" : self.number_of_assignments_available,
        "number_of_assignments_completed" : self.number_of_assignments_completed,
        "number_of_assignments_pending" : self.number_of_assignments_pending,
        "reward" : self.reward,
        "title" : self.title,
        "expired" : self.expired}
    
class TranscriptionHit(HitEntity):
    HIT_TYPE_ID = "BasicTranscriptionHitType"
    

class DisfluencyHit(HitEntity):
    HIT_TYPE_ID = "BasicDisfluencyHitType"
