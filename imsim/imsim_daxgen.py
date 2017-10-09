import os
import sys
import glob
import pwd
import time
import Pegasus.DAX3 as DAX3
import desc.imsim_deep_pipeline as idp

USER = pwd.getpwuid(os.getuid())[0]

# Create a abstract dag
dax = DAX3.ADAG("imsim_pipeline")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

dither_info_file = 'dither_info.pkl'
sensor_lists = idp.SensorLists(dither_info_file)

for visit, visit_info in sensor_lists.visits:
    band = visit_info.band
    for sensor_id in visit_info.sensor_ids:
        make_instcat = DAX3.Job('make_instcat')
        make_instcat.addArguments(visit, sensor_id)
        instcat = DAX3.File('instcat_%(visit)s_%(sensor_id)s.txt' % locals())
        make_instcat.uses(instcat, link=DAX3.Link.OUTPUT, transfer=True,
                          register=True)
        dax.addJob(make_instcat)

        run_imsim = DAX3.Job('run_imsim')
        run_imsim.uses(instcat, link=DAX3.Link.INPUT)
        dax.addJob(run_imsim)
        dax.depends(run_imsim, make_instcat)
        eimage = dax.File('lsst_e_%(visit)s_%(sensor_id)s_%(band)s.fits'
                          % locals())
