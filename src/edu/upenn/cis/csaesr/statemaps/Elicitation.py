"""Define the statemap functions here"""
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
class Comparisons(object):
    def __init__(self):
        self.map = []
        
    def greater_than_zero(self,parameter,artifact):
        return parameter in artifact and len(artifact[parameter]) > 0
    
    def equal_to_zero(self,parameter,artifact):
        return parameter in artifact and len(artifact[parameter]) == 0
    
    def alpha_numeric(self,parameter,artifact):
        return parameter in artifact and artifact[parameter].isalnum()
  
#For all classes, order of the state map matters. For the first function that fails
#The previous function's value with be taken  
class PromptSource(Comparisons):
    def __init__(self):
        self.map = ["Listed"]
        
    def Listed(self,artifact):
        return self.greater_than_zero("prompt_list", artifact)
    
    
class RecordingSource(Comparisons):
    def __init__(self):
        self.map = ["Clipped"]
        
    def Clipped(self,artifact):
        return self.greater_than_zero("clips", artifact)

            
class Prompt(Comparisons):
    def __init__(self):
        self.map = ["Queued","Hit","Recorded"]
        
    def Queued(self,artifact):
        return self.greater_than_zero("inqueue", artifact)
        
    def Hit(self,artifact):
        return self.alpha_numeric("hit_id", artifact) and\
             self.greater_than_zero("hit_id", artifact)
             
    def Recorded(self,artifact):
        return self.greater_than_zero("recording_sources", artifact)
             
class ElicitationHit(Comparisons):
    def __init__(self):
        self.map = ["Submitted"]
        
    def Submitted(self,artifact):
        return self.greater_than_zero("submitted_assignments", artifact)
    
    
class ElicitationAssignment(Comparisons):
    def __init__(self):
        self.map = ["Submitted","Approved"]
        
    def Approved(self,artifact):
        return self.greater_than_zero("approval",artifact)
    
    def Submitted(self,artifact):
        return self.greater_than_zero("recordings",artifact)
    
class Worker(Comparisons):
    def __init__(self):
        self.map = ["Submitted","Approved","Denied","Blocked"]
        
    def Submitted(self,artifact):
        return self.greater_than_zero("submitted_assignments",artifact)
        
    def Approved(self,artifact):
        return self.greater_than_zero("approved_assignments",artifact)
    
    def Denied(self,artifact):
        return self.greater_than_zero("denied_assignments",artifact)
    
    def Blocked(self,artifact):
        return self.greater_than_zero("blocked_assignments",artifact)

    
    
    
        