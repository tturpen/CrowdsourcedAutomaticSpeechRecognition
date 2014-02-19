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
class Hit(object):
    """Abstract definition of a HIT"""
    DEFAULT_TEMPLATE_PATH = "templates/default_hit.html"
    
    def __init__(self,id):
        self.id = id
        self.template_path = ""
        self.default_template = self.DEFAULT_TEMPLATE_PATH
        
    def get_template_path(self):
        if not self.template_path:
            return self.default_template
        
    def get_mongo_entry(self):
        return {"_id" : self.id,
                "template_path" : self.template_path,
                "clips" : self.clips,
                "creation_time": self.creation_time}
    def get_boto_form(self):
        
        
class TranscriptionHit(Hit):
    def __init__(self,id,template):    
        self.id = id
        self.template_path = ""
        self.default_template = template

class DislfuencyHit(Hit):
    def __init__(self,id,template):    
        self.id = id
        self.template_path = ""
        self.default_template = template
        
class SecondPassHit(Hit):
    def __init__(self,id,template):    
        self.id = id
        self.template_path = ""
        self.default_template = template

class Assignment(Hit):
    def __init__(self,id,template,hit_id):    
        self.id = id
        self.template_path = ""
        self.default_template = template