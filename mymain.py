from multiprocessing import Pipe
import os,sys; sys.path.append('.')

from section1.ETLRow import Pipeline1

job = Pipeline1()
job.main()