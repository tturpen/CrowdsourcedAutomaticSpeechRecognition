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
    
class PromptSource(Comparisons):
    def __init__(self):
        self.map = ["new","listed"]
        
    def listed(self,artifact):
        return self.greater_than_zero("prompt_list", artifact)
    
    def new(self,artifact):
        return self.equal_to_zero("prompt_list", artifact)
            
class Prompt(Comparisons):
    def __init__(self):
        self.map = ["new","hit"]
        
    def hit(self,artifact):
        return self.alpha_numeric("hit_id", artifact) and\
             self.greater_than_zero("hit_id", artifact)
    
    def new(self,artifact):
        return self.alpha_numeric("hit_id", artifact) and\
             self.equal_to_zero("hit_id", artifact)
    
        