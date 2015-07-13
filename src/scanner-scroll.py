#!/usr/bin/env python

import random, json, requests, argparse, sys, pickle, os, urllib, subprocess, time
from datetime import date

'''
Author:
    'zhang wenbo'

Date:
    7/10/2015

Function:
    Automatically check elastic search docs and upload new docs to deepdive.

Usage:
    scanner --grab "storageFileName" "indexName" "memex-username" "memex-password" "deepdive-username"
                    "deepdive-password" "deepdive-repo" "logFileName"
    scanner --dump rawDoc
    scanner --verbose rawDoc
'''

maxDocsInIndex = 100    # scrolling size for each loop, 100 is fine
sleepTime = 3600*3      # seconds, 3600*3 = 3 hours


# test parameters
LimitIdLogLength = False    # for testing, limit total id number keeped in idlog, limit to 1000 ids
DumpToLocal = False      # for testing, dump new raw file to local dir
VERBOSE = False         # for testing, verbose or not


'''

Notes:
1. diff each day 

2. use scan scroll feature to process it
    elastic .java

3. https://els.istresearch.com:9200/_cat/indices?v/memex-domains_7
use memex-domains instead of memex-domains _7
look at CDG wiki to find diff types
note index = 'memex-domains', _type='weapon'
https://els.istresearch.com:9200/memex-domains_7/weapons/_search/?q=*

4. 'insertion time', wait for upadated funcions.

5. testing script, multiple times a day to check
    a. simulate users 
    b. test new document from yesterday
    c. explore test method about web applications 

6. security, extraction
'''

def loadLogDict(filename):
    try:
        with open(filename, 'rb') as f:
            oldlog = pickle.load(f)
            f.close()
            return oldlog
    except:
        with open(filename, 'wb') as f:
            newlog = dict()
            pickle.dump( newlog, f)
            f.close()
            print 'Failed to load LogDict file. Create new log ' + filename
            return newlog

def updateLogDict(logDict, newids):
    try:
        for id in newids:
            if logDict.has_key( id ): print 'Found existing id in logDict. Something is wrong with the LogDict'
            else: logDict[ id ] = 1
        return logDict
    except: print 'Failed to update LogDict file'

def dumpLogDict(filename, logDict):
    try:
        with open(filename, 'wb') as f:
            pickle.dump(logDict, f)
            f.close()
    except: print 'Failed to dump LogDict file: ' + filename

def dumpRawDoc(filename, rawdocs):
    try:
        fname = filename
        print "Dumping rawdocs of size", len(rawdocs)
        # dump to local pickle file
        f = open(fname, "w")
        try: pickle.dump(rawdocs, f)
        finally: f.close()
    except:
        print 'Failed to dump RawDoc file: ' + filename


def getNewIds(logdict, docids, verbose = VERBOSE):
    '''
    filter existing docids, only return new ids
    '''
    try:
        newids = list()
        for id in docids:
            if not logdict.has_key( id ): newids.append( id )
        if verbose: print "Found ", len( newids ), " new ids."
        return newids
    except:
        print "Failed to filter doc ids. Upload all ids"
        return docids


def grab(docFile, username, passwd, indexName, DDusername, DDpassword, DDrepo, logfile):
    '''
    grab all the (id, contents) pairs of the given elasticsearch index,
    and send all of the (id, contents) elements to the DeepDive API for processing
    '''
    try:
        # initialize scroll-scan for Elastic search: reference/1.6/search-request-scroll.html
        getDocIdsURL = "https://els.istresearch.com:9200/" + indexName + "/_search/"
        r = requests.post(getDocIdsURL,
                          params={
                            "size": str(maxDocsInIndex),
                            "q": "*",
                            "scroll": "1m",
                            "fields": "_id",
                            "search_type": "scan"},
                          auth=(username, passwd))
        jsonObj = json.loads( r.text )
        firstHits = jsonObj[ "hits" ]
        nextScrollId = jsonObj[ "_scroll_id" ]
        maxIdNum = long(jsonObj[ "hits" ][ "total" ])

        # load existing log for indices
        logDict = loadLogDict(logfile)

        # start scroll ids and send all of the (id, contents) elements to DeepDive
        allDoc = []
        newIdNum = 0
        while len(firstHits) > 0:

            # a new scroll
            getDocIdsURL = "https://els.istresearch.com:9200/_search/scroll/"
            s = requests.post(getDocIdsURL,
                              params={
                                "scroll": "1m",
                                "scroll_id": nextScrollId},
                              auth=(username, passwd))
            jsonObj = json.loads(s.text)

            # scrolling has results
            if jsonObj.has_key( "hits" ):
                if jsonObj[ "hits" ].has_key( "hits" ):
                    allHits = jsonObj["hits"]["hits"]
                    nextScrollId = jsonObj["_scroll_id"]
                    docids = map(lambda x: str(x["_id"]), allHits)

                    # only choose new ids
                    newids = getNewIds(logDict, docids)
                    logDict = updateLogDict(logDict, newids)

                    # grab contents of new ids
                    if len(newids) > 0:
                        getDocsURL = "https://els.istresearch.com:9200/" + indexName + "/_mget/"
                        payload = {"ids": newids}
                        r = requests.post(getDocsURL, params={"size": str(maxDocsInIndex)}, auth=(username, passwd), data=json.dumps(payload))
                        jsonObj = json.loads(r.text)
                        docs = jsonObj["docs"]
                        docContents = map(lambda x: (x["_id"], x["_source"]["url"], x["_source"]["raw_content"]), docs)

                        # send all of the (id, contents) elements to DeepDive
                        extract(docContents, DDusername, DDpassword, DDrepo)
                        newIdNum = newIdNum + len(newids)

                        # save all of the (id, contents) elements to Local file
                        if DumpToLocal:
                            for doc in docContents:
                                allDoc.append(doc)
                else: break
            else: break

            # limit for test
            if LimitIdLogLength:
                if len(logDict) > 1000: #maxIdNum + 10:
                    break

        print "\nFinished\n", "Total existing id number:", len(logDict)
        print "Uploaded ", newIdNum, "new docs to Deepdive"

        dumpLogDict(logfile, logDict)

        # save all of the (id, contents) elements to Local file
        if DumpToLocal:
            dumpRawDoc(docFile, allDoc)
    except: print "Failed to get doc contents."

def extract(rawdocs, username, password, repo):
    '''
    extract() will send all of the (id, contents) elements to the DeepDive API for processing
    '''
    try:
        # Grab the token authorization
        authorizeURL = "https://api.clearcutcorp.com/api-token-auth/"
        r = requests.post(authorizeURL, data={"username": username, "password": password})
        jsonObj = json.loads(r.text)
        print '[jsonObj] ', jsonObj
        token = jsonObj["token"]

        # Upload the docs
        strlist = map(lambda rd: json.dumps({"docid": rd[0], "url": rd[1], "content": rd[2]}), rawdocs)
        totalDatasetToExtract = "\n".join(strlist)
        f = open("./tempUpload", "w")
        try:
            f.write(totalDatasetToExtract)
        finally:
            f.close()

        # Copy file contents to 'totalDataURL'
        randFname = "fname_rand_" + str(random.randint(0, 10000))
        s3URL = "s3://deepdive-private/temp-api/" + randFname
        totalDataURL = "http://s3.amazonaws.com/deepdive-private/temp-api/" + randFname
        subprocess.call(["aws", "s3", "cp", "./tempUpload", s3URL])

        # Ask DeepDive to process it
        extractURL = "https://api.clearcutcorp.com/sources/" + username + "/" + repo + "/"
        r = requests.post(extractURL, headers={"Authorization": "Token " + token}, data={"source_url": totalDataURL})
        print '[Return Message] ', r.text

    except: print "Failed to send doc to Deepdive."

def getDumpDocs(fname):
    f = open(fname)
    try:
        docContents = pickle.load(f)
        print "Size of loaded elt", len(docContents)
        for lineno, content in enumerate(docContents):
            id, url, encodedContent = content
            try:
                print lineno, id, url, "..."
                print
            except:
                pass
    finally:
        f.close()

def getDumpDocsVerbose(fname):
    f = open(fname)
    try:
        docContents = pickle.load(f)
        print "Size of loaded elt", len(docContents)
        for lineno, content in enumerate(docContents):
            id, url, encodedContent = content
            try:
                print lineno, id, url, content
                print
            except:
                pass
    finally:
        f.close()

if __name__ == "__main__":
    startTime = time.time()

    usage = "usage %prog [options]"
    parser = argparse.ArgumentParser(
        description="for extracting content from the common crawl via the incremental API")
    parser.add_argument("--grab",
                      nargs=8,
                      metavar=("storageFileName", "indexName", "memex-username", "memex-password", "deepdive-username",
                               "deepdive-password", "deepdive-repo", "logFileName"),
                      help="To simply grab and store")
    parser.add_argument("--dump",
                      nargs=1,
                      metavar=("storageFile"),
                      help="Dump document data")
    parser.add_argument("--verbose",
                      nargs=1,
                      metavar=("storageFile"),
                      help="Verbosely dump document data")
    args = parser.parse_args()

    try:
        if args.grab is not None:
            # grab raw docs
            while(True):
                grab(args.grab[0], args.grab[2], args.grab[3], args.grab[1],
                    args.grab[4], args.grab[5], args.grab[6], args.grab[7])
                time.sleep( sleepTime )
        elif args.dump is not None:
            getDumpDocs(args.dump[0])
        elif args.verbose is not None:
            getDumpDocsVerbose(args.dump[0])
        else:
            parser.print_help()
    finally:
        pass

    print 'Running time: ', time.time() - startTime