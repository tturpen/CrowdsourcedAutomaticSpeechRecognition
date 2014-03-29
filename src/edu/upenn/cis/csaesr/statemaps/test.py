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
import unittest
from statemaps.Elicitation import Prompt

class Test(unittest.TestCase):

    def setUp(self):
        self.prompt = Prompt()
         
    def test_prompt_source_list_mock(self):
        map = self.prompt.map
        pos_artifact = {"prompt_list": [1,2,3]}
        neg_artifact = {"prompt_list": []}
        self.assert_(self.prompt.listed(pos_artifact))
        self.assert_(not self.prompt.listed(neg_artifact))
        
    def test_prompt_source_new_mock(self):
        map = self.prompt.map
        neg_artifact = {"prompt_list": [1,2,3]}
        pos_artifact = {"prompt_list": []}
        self.assert_(self.prompt.new(pos_artifact))
        self.assert_(not self.prompt.new(neg_artifact))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()