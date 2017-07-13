import os
import sys
import glob
import pwd
import time
from Pegasus.DAX3 import *

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

# Ingest the raw images.
ingestImages = Job('ingestImages.py')
file_names = glob.glob(os.path.join(input_repo, 'lsst_a_*.fits*'))
ingestImages.addArguments(input_repo, '--output', output_repo, *file_names)
dax.addJob(ingestImages)

# Ingest the reference catalog.
ref_cat = '/global/homes/d/descdm/dc1/DC1-imsim-dithered/dc1_reference_catalog.txt'
ingestReferenceCatalog = Job('ingestReferenceCatalog.py')
ingestReferenceCatalog.addArguments(ref_cat, output_repo)
dax.addJob(ingestReferenceCatalog)
dax.depends(ingestReferenceCatalog, ingestImages)

makeDiscreteSkyMap = Job('makeDiscreteSkyMap.py')
makeDiscreteSkyMap.addArguments(output_repo)
dax.addJob(makeDiscreteSkyMap)
# Loop over visits
for visit in visit_list(output_repo):
    # Loop over rafts
    for raft in raft_list(visit):
        processCcd = Job('processCcd.py')
        processCcd.addArguments(output_repo, '--id', 'visit=%s' % visit,
                                'raft=%s' % raft)
        dax.addJob(processCcd)
        dax.depends(processCcd, ingestReferenceCatalog)
        dax.depends(makeDiscreteSkyMap, processCcd)

# Loop over tracts
for tract in tract_list(output_repo):
    # Loop over patches.
    for patch in patch_list(output_repo, tract=tract):

        mergeDetections = Job('mergeDetections.py')
        mergeDetections.addArguments(output_repo, '--id', 'patch=%s' % patch,
                                     'tract=%s' % tract,
                                     'filter=%s' % '^'.join(filter_list()))
        dax.addJob(mergeDetections)
        for filt in filter_list():
            args = ('--id', 'filter=%s' % filt, 'patch=%s' % patch,
                    'tract=%s' % tract)
            makeTempExpCoadd = Job('makeCoaddTempExp.py')
            makeTempExpCoadd.addArguments(output_repo,
                                          '--selectId', 'filter=%s' % filt,
                                          *args)
            dax.addJob(makeTempExpCoadd)
            dax.depends(makeTempExpCoadd, makeDiscreteSkyMap)

            assembleCoadd = Job('assembleCoadd.py')
            assembleCoadd.addArguments(output_repo,
                                       '--selectId', 'filter=%s' % filt, *args)
            dax.addJob(assembleCoadd)
            dax.depends(assembleCoadd, makeTempExpCoadd)

            detectCoaddSources = Job('detectCoaddSource.py')
            detectCoaddSources.addArguments(output_repo, *args)
            dax.addJob(detectCoaddSources)
            dax.depends(detectCoaddSources, assembleCoadd)

            dax.depends(mergeDetections, detectCoaddSources)

        # Make a separate loop over filters for measureCoadd job
        # since it will take place after mergeDetections has run on
        # all filters.
        for filt in filter_list():
            measureCoadd = Job('measureCoadd.py')
            measureCoadd.addArguments(output_repo, *args)
            dax.addJob(measureCoadd)
            dax.depends(measureCoadd, mergeDetections)

# Forced photometry on data for each visit.
for visit in visit_list(output_repo):
    for raft in raft_list(visit):
        for sensor in sensor_list(visit, raft):
            forcedPhotCcd = Job('forcedPhotCcd.py')
            forcedPhotCcd.addArguments(output_repo, '--id', 'visit=%s' % visit,
                                       'raft=%s' % raft, 'sensor=%s' % sensor)

daxfile = 'L2.dax'
with open(daxfile, 'w') as f:
    dax.writeXML(f)
