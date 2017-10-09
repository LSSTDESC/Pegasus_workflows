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
dax = DAX3.ADAG("Level_2_calexp_Pipeline")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

input_repo = '/global/cscratch1/sd/descdm/DC1/DC1-imsim-dithered'
output_repo = '.'
config_dir = './configs'

job_maker = JobMaker(dax, output_repo, config_dir, bin_dir='./bin', tc='tc.txt',
                     clobber=True)

# Ingest the raw images.
ingestImages = job_maker.make('ingestImages', repo=input_repo,
                              options={'--output': output_repo})

# Ingest the reference catalog.
ref_cat = '/global/homes/d/descdm/dc1/DC1-imsim-dithered/dc1_reference_catalog.txt'
ingestReferenceCatalog = DAX3.Job('ingestReferenceCatalog')
ingestReferenceCatalog.addArguments(ref_cat, output_repo)
dax.addJob(ingestReferenceCatalog)
dax.depends(ingestReferenceCatalog, ingestImages)
job_maker.add_tc_entry(job_maker, 'ingestReferenceCatalog')

makeDiscreteSkyMap = job_maker.make('makeDiscreteSkyMap')
# Loop over visits
for visit in visit_list(output_repo):
    # Loop over rafts
    for raft in raft_list(visit):
        dataId = dict(visit=visit, raft=raft)
        processCcd = job_maker.make('processCcd', dataId=dataId)
        dax.depends(processCcd, ingestReferenceCatalog)
        dax.depends(makeDiscreteSkyMap, processCcd)

daxfile = 'L2_calexp.dax'
with open(daxfile, 'w') as f:
    dax.writeXML(f)
