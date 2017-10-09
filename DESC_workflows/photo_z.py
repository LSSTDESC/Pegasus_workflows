import os
import pwd
import time
import Pegasus.DAX3 as DAX3

USER = pwd.getpwuid(os.getuid())[0]

# Create a abstract dag
dax = DAX3.ADAG("Photo-Z Pipeline")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

training_samples = DAX3.File('spec_z_training_samples')
pz_training = DAX3.Job('pz_training')
pz_training.uses(training_samples, link=DAX3.Link.INPUT)
dax.addJob(pz_training)

lsst_mags = DAX3.File('lsst_mags')
photo_z_funcs = DAX3.File('photo_z_funcs')
photo_z_code = DAX3.Job('photo_z_code')
photo_z_code.uses(lsst_mags, link=DAX3.Link.INPUT)
photo_z_code.uses(photo_z_funcs, link=DAX3.Link.OUTPUT, transfer=True,
                  register=True)
dax.addJob(photo_z_code)

dax.depends(photo_z_code, pz_training)

calibration_samples = DAX3.File('spec_z_calibration_samples')
photo_z_stats = DAX3.File('photo_z_statistics')
cross_corr_calibration = DAX3.Job('cross_corr_calibration')
cross_corr_calibration.uses(photo_z_funcs, link=DAX3.Link.INPUT)
cross_corr_calibration.uses(photo_z_stats, link=DAX3.Link.OUTPUT, transfer=True,
                            register=True)
dax.addJob(cross_corr_calibration)
dax.depends(cross_corr_calibration, photo_z_code)

daxfile = 'photo_z.dax'
with open(daxfile, 'w') as f:
    dax.writeXML(f)
print "Generated dax %s" % daxfile
