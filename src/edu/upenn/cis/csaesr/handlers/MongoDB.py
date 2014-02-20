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

class MongoHandler(object):
    default_db_loc = 'mongodb://localhost:27017'

    def __init__(self):
        client = MongoClient(self.default_db_loc)        
        self.db = client.transcription_db
        self.audio_sources = self.db.audio#The full audio files
        self.audio_clips = self.db.audio_clips#portions of the full audio files
        self.transcripts = self.db.transcripts
        self.transcription_hits = self.db.transcription_hits
        self.disfluency_hits = self.db.disfluency_hits
        self.second_pass_hits = self.db.second_pass_hits
        self.turkers = self.db.turkers
        self.type_ids = self.db.type_ids
        
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
        if len(response) > 1:
            raise TooManyEntries
        return response[0][field]
    
    def get_audio_clip_url(self,audio_clip_id):
        return self.audio_clip_find_one({"_id" : audio_clip_id},"audio_clip_url")
    
    def get_audio_clip_status(self,audio_clip_id):
        return self.audio_clip_find_one({"_id" : audio_clip_id},"Status")
    
def main():
    mh = MongoHandler()
    t = Turker(5)
    print(t.get_mongo_entry())
    mh.insert_entity(t)
    
if __name__ == "__main__":
    main()
        
       
        
        
    