import os
import sys
import glob
import pwd
import time
import Pegasus.DAX3 as DAX3
from JobMaker import JobMaker
from repo_tools import *

USER = pwd.getpwuid(os.getuid())[0]

# Create a abstract dag
dax = DAX3.ADAG("Level_2_Coadd_Pipeline")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

input_repo = '/global/cscratch1/sd/descdm/DC1/DC1-imsim-dithered'
output_repo = '.'
config_dir = './configs'

job_maker = JobMaker(dax, output_repo, config_dir, bin_dir='./bin', tc='tc.txt')

# Loop over tracts
for tract in tract_list(output_repo):
    # Loop over patches.
    for patch in patch_list(output_repo, tract=tract):
        dataId = dict(patch=patch, tract=tract, filter='^'.join(filter_list()))
        mergeDetections = job_maker.make('mergeDetections', dataId=dataId)
        for filt in filter_list():
            dataId = dict(patch=patch, tract=tract, filter=filt)
            options = {'--selectId': 'filter=%s' % filt}
            makeTempExpCoadd = job_maker.make('makeTempExpCoadd', dataId=dataId,
                                              options=options)
#            dax.depends(makeTempExpCoadd, makeDiscreteSkyMap)

            assembleCoadd = job_maker.make('assembleCoadd', dataId=dataId,
                                           options=options)
            dax.depends(assembleCoadd, makeTempExpCoadd)

            detectCoaddSources = job_maker.make('detectCoaddSources',
                                                dataId=dataId)
            dax.depends(detectCoaddSources, assembleCoadd)
            dax.depends(mergeDetections, detectCoaddSources)

        # Make a separate loop over filters for measureCoadd job
        # since it will take place after mergeDetections has run on
        # all filters.
        for filt in filter_list():
            dataId = dict(patch=patch, tract=tract, filter=filt)
            measureCoadd = job_maker.make('measureCoadd', dataId=dataId)
            dax.depends(measureCoadd, mergeDetections)

# Forced photometry on data for each visit.
for visit in visit_list(output_repo):
    for raft in raft_list(visit):
        for sensor in sensor_list(visit, raft):
            dataId = dict(visit=visit, raft=raft, sensor=sensor)
            forcedPhotCcd = job_maker.make('forcedPhotCcd', dataId=dataId)

daxfile = 'L2_coadd.dax'
with open(daxfile, 'w') as f:
    dax.writeXML(f)
