import os
import pwd
import time
import Pegasus.DAX3 as DAX3

USER = pwd.getpwuid(os.getuid())[0]

# Create a abstract dag
dax = DAX3.ADAG("Strong Lensing Pipeline")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

dm_level1_catalog = DAX3.File('dm_level1_catalog')
dm_images = DAX3.File('dm_image_data')
SL_candidates = DAX3.File('SL_candidates')
SLFinder = DAX3.Job('SLFinder')
SLFinder.uses(dm_level1_catalog, link=DAX3.Link.INPUT)
SLFinder.uses(dm_images, link=DAX3.Link.INPUT)
SLFinder.uses(SL_candidates, link=DAX3.Link.OUTPUT, register=True,
              transfer=True)
dax.addJob(SLFinder)

DESC_Lenses = DAX3.File('DESC_Lenses')
SpaceWarps = DAX3.Job('SpaceWarps')
SpaceWarps.uses(SL_candidates, link=DAX3.Link.INPUT)
SpaceWarps.uses(DESC_Lenses, link=DAX3.Link.OUTPUT, register=True,
                transfer=True)
dax.addJob(SpaceWarps)
dax.depends(SpaceWarps, SLFinder)

SL_light_curves = DAX3.File('SL_light_curves')
SLMonitor = DAX3.Job('SLMonitor')
SLMonitor.uses(DESC_Lenses, link=DAX3.Link.INPUT)
SLMonitor.uses(dm_images, link=DAX3.Link.INPUT)
SLMonitor.uses(SL_light_curves, link=DAX3.Link.OUTPUT, register=True,
               transfer=True)
dax.addJob(SLMonitor)
dax.depends(SLMonitor, SpaceWarps)

SL_time_delays = DAX3.File('SL_time_delays')
SLTimer = DAX3.Job('SLTimer')
SLTimer.uses(SL_light_curves, link=DAX3.Link.INPUT)
SLTimer.uses(SL_time_delays, link=DAX3.Link.OUTPUT, register=True,
             transfer=True)
dax.addJob(SLTimer)
dax.depends(SLTimer, SLMonitor)

WL_shapes = DAX3.File('WL_shapes')
Photo_zs = DAX3.File('Photo_zs')
cosmo_sims = DAX3.File('cosmo_sims')
WL_params = DAX3.File('WL_params')
SLENV = DAX3.Job('SLENV')
SLENV.uses(WL_shapes, link=DAX3.Link.INPUT)
SLENV.uses(Photo_zs, link=DAX3.Link.INPUT)
SLENV.uses(cosmo_sims, link=DAX3.Link.INPUT)
SLENV.uses(WL_params, link=DAX3.Link.OUTPUT, register=True, transfer=True)
dax.addJob(SLENV)
dax.depends(SLENV, SpaceWarps)

Follow_up_data = DAX3.File('Follow_up_data')
lens_models = DAX3.File('lens_models')
SLModeler = DAX3.Job('SLModeler')
SLModeler.uses(SL_time_delays, link=DAX3.Link.INPUT)
SLModeler.uses(Follow_up_data, link=DAX3.Link.INPUT)
SLModeler.uses(lens_models, link=DAX3.Link.OUTPUT, register=True, transfer=True)
dax.addJob(SLModeler)
dax.depends(SLModeler, SLENV)

sl_cosmo_params = DAX3.File('sl_cosmo_params')
SLCosmo = DAX3.Job('SLCosmo')
SLCosmo.uses(lens_models, link=DAX3.Link.INPUT)
SLCosmo.uses(sl_cosmo_params, link=DAX3.Link.OUTPUT, register=True,
             transfer=True)
dax.addJob(SLCosmo)
dax.depends(SLCosmo, SLModeler)
