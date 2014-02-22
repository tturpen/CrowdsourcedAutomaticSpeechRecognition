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

def main():
    ref = "Humpty dumpty broke his head."
    hyp = "Humpty broke his nose."
    print(wer_2(ref.split(),hyp.split()))
    
def wer(ref,hyp):
    d = []
    #zeros
    for j in range(len(ref)+1):
        d.append([0]*(len(hyp)+1))
    #initialize
    for i in range(len(d)):
        for j in range(len(d[0])):
            if i == 0:
                d[0][j] = j
            elif j == 0:
                d[i][0] = i
    #calculate            
    for i in range(1, len(d)):
        for j in range(1, len(d[0])):
            if ref[i-1] == hyp[j-1]:
                d[i][j] = d[i-1][j-1]
            else:
                sub = d[i-1][j-1] + 1
                ins = d[i][j-1] + 1
                deletion = d[i-1][j] + 1
                d[i][j] = min(sub,ins,deletion)
    return d[len(ref)][len(hyp)]

if __name__ == "__main__":
    main()