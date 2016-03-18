from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

"""
    Obtain word frequency and produce a list of words sorted in ascending 
    order from the least to the most frequently used word. 
    The class uses 2 steps, hence it needs two mapper functions 
    and 2 reducer functions.
    To run the program, execute the following command:
    1) Locally from the editor console:
        !python word_frequency_sorted.py word_frequency_book.txt > wfs.txt
    2) In the AWS cloud from the Canopy command terminal:
        python word_frequency_sorted.py -r emr 
        --conf-path=C:\Users\[user name]\.mrjob.config  
        word_frequency_book.txt > wfs.txt

"""
class MRWordFrequencyCount(MRJob):

    """
        Define the required steps in the order they are executed. 
        The first step uses 
            a) the mapper_get_words function to map the words (to 1). 
            b) the reducer_count_words function to get the words count 
               (frequency)
        The second step takes the output of the first reducer and uses 
            a) the mapper_make_counts_key function to flip the key and values
               and pad the count with 0s (zeroes) so the entries can be properly
               sorted.                
            b) the reducer_output_words function to output the result pretty
               much. 
    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_make_counts_key,
                   reducer = self.reducer_output_words)
        ]


    """
        Map every word from the input file to the number 1.
        Notice that because a particular word can be found multiple times,  
        the mapper function associates a word with the number 1 as many times 
        a word appears.Every time a word appears the mapper produces a key, 
        value pair where the key is the word and the value is 1.
        For instance:
            mom, 1 mom, 1 mom, 1 dad, 1 dad, 1 dog, 1 and so forth.
       
        Input:  _ - Ignore
                line - A comma separeted input line (with line termination \t)
        Output: List of key, value pairs holding words and 1 
                
    """    
    def mapper_get_words(self, _, line):
        " Search for words in each line."
        words = WORD_REGEXP.findall(line)
        for word in words:
            " Avoids issues in mrjob 5.0 "
            word = unicode(word, "utf-8", errors="ignore") 
            " Yield a list of word, 1 pairs. "
            yield word.lower(), 1
            
    
    """
        Calculate the number of times a word appears.
        Input:  word - Word value (key) that was yielded by the mapper.
                values - A generator which yields all 1s
                        yielded by the mapper which correspond to the word.
        Output: List of key, value pairs holding word and the total number 
                it appears.
    """
    def reducer_count_words(self, word, values):
        " Evaluate the number of times a word appears."
        yield word, sum(values)

    """
        For each key, value pair, format the counter as 4 digit integer.
        Input:  word - Word value (key) that was yielded by the mapper.
                count - The number of times a word appears.
        Output: List of key, value pairs holding word and the total number 
                it appears.
    """
    def mapper_make_counts_key(self, word, count):
        yield '%04d'%int(count), word

    """
        Sort the key, value pairs by the count.
        This allows to create a list of words with the most frequent
        ones listed first. That is in ascending order based on the count. 
        Input:  words - List of words yielded by the mapper.
                count - The number of times a word appears.
        Output: List of key, value pairs holding word and the total number 
                it appears.
    """
    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word


if __name__ == '__main__':
    MRWordFrequencyCount.run()
