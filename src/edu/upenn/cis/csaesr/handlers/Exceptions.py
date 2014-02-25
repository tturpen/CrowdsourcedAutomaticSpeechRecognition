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
class MultipleResultsOnIdFind(Exception):
    """Raise if a find on an ID returns multiple results"""
    def __init__(self,message):
        return str("Multiple results for ID find on ID: %s"%message)
    
class IncorrectTextFieldCount(Exception):
    """Raise if there is an incorrect number of text fields in 
        the QuestionFormAnswer"""
    def __init__(self,message):
        raise
    
class TooManyEntries(Exception):
    """Raise if there is an incorrect number of entries"""
    def __init__(self,message):
        raise
    
class WavHandlerException(Exception):
    """If something goes wrong with handling wav files, raise"""
    def __init__(self,message):
        raise
    
class DuplicateSentenceIds(Exception):
    """Raise if there are duplicate sentence ids in the prompts"""
    def __init__(self,message):
        raise    
    
class PromptNotFound(Exception):
    """Raise if a prompt for an audio source cannot be found"""
    def __init__(self,message):
        raise
    




    
        