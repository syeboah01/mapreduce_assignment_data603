"""
filename: mapreduce2.py
Author: Sheila Yeboah
Due Date: 9/24/25
Assignment: MapReduce Assignment Part 1
Description: Using Mapreduce, search through text from the Harry Potter book series. Part 2
Task: Count how many times non-English words (names, places, spells etc.) were used. List those words and how many times each was repeated.
"""
import re
from mrjob.job import MRJob
from spellchecker import SpellChecker

# initiate python spellchecker 
spell = SpellChecker() 

# match only words and contractions, without getting quotes or other non-words
WORD_RE = re.compile(r"\b[\w']+\b")


class MRWordFreqCount(MRJob):
    # mapper takes every word and gives its occurence a 1
    def mapper(self, _, line):
       # match non utf-8 quotes (aka smart quotes) and replace with utf-8 quote
       line = line.replace('\u2019', "'")
       for word in WORD_RE.findall(line):
            # if the word fails spellcheck, yield it to retain non-english words
            if spell.unknown([word]):
                yield word.lower(), 1
    # combiner takes mapper output and sums it, so each word occurence appears once with a total
    def combiner(self, word, counts):
        yield word, sum(counts)
    # reducer takes totals from all soures and sums again, in case of a single step job, this only comes from combiner
    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == '__main__':
    MRWordFreqCount.run()
