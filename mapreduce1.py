"""
Filename: mapreduce1.py
Author: Sheila Yeboah
Due Date: 9/24/25
Assignment: MapReduce Assignment
Description: Using Mapreduce, search through text from the Harry Potter book series. Part 1
Task: Write Python code and use MapReduct to count occurrences of each word in the first text file (file.txt). 
How many times each word is repeated?
"""


"""
my birthday is 5/5/2001 so I picked the 5th book, 
Harry Potter and the Sorcerer's Stone, started from the 5th page for file1 and the 101st page for book 2
"""
 

# code adapted from mrjobs docs https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html#single-step-jobs 

import re
from mrjob.job import MRJob


# match only words and contractions, without getting quotes or other non-words
WORD_RE = re.compile(r"\b[\w']+\b")


class MRWordFreqCount(MRJob):
    
    # mapper takes every word and gives its occurence a 1
    def mapper(self, _, line):
       # match Unicode apostrophe and replace with standard ASCII apostrophe so we include contractions
       line = line.replace('\u2019', "'")
       for word in WORD_RE.findall(line):
            yield word.lower(), 1

    # combiner takes mapper output and sums it, so each word occurence appears once with a total
    def combiner(self, word, counts):
        yield word, sum(counts)

    # reducer takes totals from all soures and sums again, in case of a single step job, this only comes from combiner
    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == '__main__':
    MRWordFreqCount.run()
