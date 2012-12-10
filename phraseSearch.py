#!/usr/bin/env python
# -*- coding:utf-8
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
import sys
import json
import codecs
from etool import args, logs
from datetime import datetime
from unidecode import unidecode

__processor__ = 'PhraseFilter'
logger = None
phraseList = []
phraseConf = []


def generatePhraseList(phraseStrArray):
    '''Convert input phrases to internal Phrase format.'''
    for ph in phraseStrArray:
        parts = unicode(unidecode(ph.strip())).split('\t')
        if len(parts) == 2:
            conf = float(parts[1])
        else:
            conf = 0.5
        phrase = eval(parts[0])
        if not(type(phrase[-1]) == int or type(phrase[-1]) == float):
            phrase.append(3)  # Default minDistance Value
        phraseList.append(phrase)
        phraseConf.append(conf)

def getPhrase(phraseStr):
    global phraseList, phraseConf
    phraseList = phraseStr
    phraseConf = [0]*len(phraseList)

def filterByPhrase(sentence, order=True):
    '''Check for presence of phrases'''
    #phrasesPresent = [(k, [[token[0] for token in enumerate(sentence[0]) if kl[0] == token[1][kl[1]]] for kl in k[0:-1]]) for k in phraseList]
    phrasePresent = False
    for phrase in phraseList:
        #raw_input('next - %s' % phrase)
        minDistance = phrase[-1]
        sLen = len(sentence[0])
        distance = 0
        i = 0
        lastPos = 0
        flag = False
        if order:
            wordSeen = []
            wordNotSeen = phrase[1:-1]
            wordReq = phrase[0]
            while i < sLen:
                word = sentence[0][i]
                if flag:
                    break
                if unicode(word[wordReq[1]]) == unicode(wordReq[0]):
                    if len(wordReq) == 3:
                        if word['POS'] != wordReq[2]:
                            i += 1
                            continue

                    if lastPos == 0:
                        lastPos = i
                        wordSeen.append(wordReq)
                        if wordNotSeen:
                            wordReq = wordNotSeen[0]
                            wordNotSeen.remove(wordReq)
                    else:
                        distance = i - lastPos
                        if distance < minDistance:
                            lastPos = i
                            wordSeen.append(wordReq)
                            if wordNotSeen != []:
                                wordReq = wordNotSeen[0]
                                wordNotSeen.remove(wordReq)
                        else:
                            lastPos = 0
                            wordSeen = []
                            wordReq = phrase[0]
                            wordNotSeen = phrase[1:-1]

                elif any([k for k in wordSeen if unicode(k[0]) == unicode(word[k[1]])]) or any([k for k in wordNotSeen if unicode(k[0]) == unicode(word[k[1]])]):
                    wordSeen = []
                    wordNotSeen = phrase[1:-1]
                    wordReq = phrase[0]

                if wordSeen == phrase[0:-1]:
                    flag = True

                i += 1

            if flag:
                return (phrase, i)

        else:
            wordSeen = []
            wordNotSeen = phrase[0:-1]
            #wordReq = phrase[0]

            while i < sLen:
                word = sentence[0][i]
                nextWord = [k for k in phrase[0:-1] if unicode(k[0]) == unicode(word[k[1]])]
                if len(k) == 3:
                    if word['POS'] != k[2]:
                        nextWord == []

                if nextWord == []:
                    i += 1
                    continue
                i += 1
                if nextWord[0] in wordSeen:
                    distance = 0
                    wordSeen = []
                    wordNotSeen = phrase[0:-1]
                    continue
                else:
                    wordSeen.append(nextWord[0])
                    wordNotSeen.remove(nextWord[0])
                    if len(wordSeen) == 1:
                        distance = 0
                        lastPos = i
                    else:
                        newDist = i - lastPos
                        if newDist > minDistance:
                            wordSeen = nextWord
                            wordNotSeen = phrase[0:-1]
                            wordNotSeen.remove(nextWord[0])
                            distance = 0
                            lastPos = i
                        else:
                            if newDist > distance:
                                distance = newDist
                                lastPos = i
                if wordNotSeen == []:
                    phrasePresent = True
                    break
            if phrasePresent:
                phrasePresent = False
                return (phrase, i)  # i is the offset of phrase within sentence
    return (False, None)


def phraseSearch(reader, writer):
    '''Loop through input articles and output articles containing the phrase'''
    jLoads = json.loads
    jDumps = json.dumps
    write = writer.write
    flush = writer.flush
    j = 0
    for line in reader:
        j += 1
        article = unidecode(line)
        try:
            articleJson = jLoads(unidecode(article))
        except ValueError:
            logger.debug("unable to load json line number %s" % j)
            continue
        tokens = articleJson['BasisEnrichment']['tokens']
        sOffsets = [k[0] for k in enumerate(tokens) if k[1]['POS'] == 'SENT']
        sOffsets.insert(0, 0)
        sOffsets.append(len(tokens) - 1)
        sentences = [[tokens[sOffsets[i]:sOffsets[i + 1]]] for i in range(0, len(sOffsets) - 1)]
        results = map(filterByPhrase, sentences)
        if any([k for k, v in results]):
            offSets = [sOffsets[offs] + k[1] for offs, k in enumerate(results) if k[0] is not False]
            #print "results %s " % offSets
            results = [k[0] for k in results if k[0] is not False]
            articleJson['phrasesPresent'] = {"phrases": results, "offsets": offSets,
                                             "conf": [phraseConf[phraseList.index(k)] for k in results]}
            write(jDumps(articleJson))
            write('\n')
            flush()
    return None


def main():
    ap = args.get_parser()
    ap.add_argument('-i', '--input', default='sys.stdin', type=str, help='Path to the input file.'
                    'Default is sys.stdin')
    ap.add_argument('-o', '--out', default='sys.stdout', type=str, help='Path to the output file.'
                    'Default is sys.stdout')
    ap.add_argument('searchPhrase', default='config/phrases.txt', type=str, help='Path to '
                    'the Phrase File if "-f" flag is specified, else the input string is considered'
                    'to be the phrase.')
    ap.add_argument('-f', '--file', action='store_true', default=False, help='If given, then the '
                    'the searchPhrase argument is interpreted as path to a file')
    global logger
    logger = logs.getLogger("%s-%s.log" % (__processor__, str(datetime.now())))
    arg = ap.parse_args()
    logs.init(args)
    inputFile = None
    outFile = None
    phraseFile = None

    if arg.input == 'sys.stdin':
        reader = codecs.getreader('utf-8')(sys.stdin)
    else:
        inputFile = open(arg.input, "r")
        reader = codecs.getreader('utf-8')(inputFile)
    if arg.out == 'sys.stdout':
        writer = codecs.getwriter('utf-8')(sys.stdout)
    else:
        outFile = codecs.open(arg.out, "w", encoding="utf-8")
        writer = codecs.getwriter('utf-8')(outFile)
    if arg.file:
        phraseFile = codecs.open(arg.searchPhrase, encoding='utf-8')
        generatePhraseList(phraseFile.readlines())
    else:
        generatePhraseList([arg.searchPhrase])
    phraseSearch(reader, writer)
    #close all files
    if inputFile:
        inputFile.close()
    if outFile:
        outFile.close()
    if phraseFile:
        phraseFile.close()

if __name__ == "__main__":
    main()
