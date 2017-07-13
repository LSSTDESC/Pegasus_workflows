import os
import sys
import glob
import pwd
import time
from Pegasus.DAX3 import *
from JobMaker import JobMaker

def visit_list(repo):
    # This is a place-holder for now, to be replaced by DC2 visit list.
    return [1, 2, 3]

def raft_list(visit):
    # Down-selected list of rafts to analyze.
    return ['2,2']

def sensor_list(vist, raft):
    # Down-selected list of sensors.
    return ['1,1']

def tract_list(repo):
    return ['0']

def patch_list(repo, tract=0):
    # List of patches for the requested repo and tract.
    return ['0,0']

def filter_list():
    return [filt for filt in 'ugrizy']

USER = pwd.getpwuid(os.getuid())[0]

# Create a abstract dag
dax = ADAG("Level_2_Pipeline")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

input_repo = '/global/cscratch1/sd/descdm/DC1/DC1-imsim-dithered'
output_repo = '.'
config_dir = './configs'

#job_maker = JobMaker(dax, output_repo, config_dir)
#
## Ingest the raw images.
#ingestImages = job_maker.make('ingestImages', repo=input_repo,
#                              options={'--output': output_repo})
#
## Ingest the reference catalog.
#ref_cat = '/global/homes/d/descdm/dc1/DC1-imsim-dithered/dc1_reference_catalog.txt'
#ingestReferenceCatalog = Job('ingestReferenceCatalog.py')
#ingestReferenceCatalog.addArguments(ref_cat, output_repo)
#dax.addJob(ingestReferenceCatalog)
#dax.depends(ingestReferenceCatalog, ingestImages)
#
#makeDiscreteSkyMap = job_maker.make('makeDiscreteSkyMap')
## Loop over visits
#for visit in visit_list(output_repo):
#    # Loop over rafts
#    for raft in raft_list(visit):
#        dataId = dict(visit=visit, raft=raft)
#        processCcd = job_maker.make('processCcd', dataId=dataId)
#        dax.depends(processCcd, ingestReferenceCatalog)
#        dax.depends(makeDiscreteSkyMap, processCcd)
#
## Loop over tracts
#for tract in tract_list(output_repo):
#    # Loop over patches.
#    for patch in patch_list(output_repo, tract=tract):
#        dataId = dict(patch=patch, tract=tract, filter='^'.join(filter_list()))
#        mergeDetections = job_maker.make('mergeDetections', dataId=dataId)
#        for filt in filter_list():
#            dataId = dict(patch=patch, tract=tract, filter=filt)
#            options = {'--selectId': 'filter=%s' % filt}
#            makeTempExpCoadd = job_maker.make('makeTempExpCoadd', dataId=dataId,
#                                              options=options)
#            dax.depends(makeTempExpCoadd, makeDiscreteSkyMap)
#
#            assembleCoadd = job_maker.make('assembleCoadd', dataId=datId,
#                                           options=options)
#            dax.depends(assembleCoadd, makeTempExpCoadd)
#
#            detectCoaddSources = job_maker.make('detectCoaddSources',
#                                                dataId=dataId)
#            dax.depends(detectCoaddSources, assembleCoadd)
#            dax.depends(mergeDetections, detectCoaddSources)
#
#        # Make a separate loop over filters for measureCoadd job
#        # since it will take place after mergeDetections has run on
#        # all filters.
#        for filt in filter_list():
#            dataId = dict(patch=patch, tract=tract, filter=filt)
#            measureCoadd = job_maker.make('measureCoadd', dataId=dataId)
#            dax.depends(measureCoadd, mergeDetections)
#
## Forced photometry on data for each visit.
#for visit in visit_list(output_repo):
#    for raft in raft_list(visit):
#        for sensor in sensor_list(visit, raft):
#            dataId = dict(visit=visit, raft=raft, sensor=sensor)
#            forcedPhotCcd = job_maker.make('forcedPhotCcd', dataId=dataId)
#
#daxfile = 'L2.dax'
#with open(daxfile, 'w') as f:
#    dax.writeXML(f)
