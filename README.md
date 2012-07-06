hailcam
=======

A webcam-based tool for testing object storage services such as Hail

todo:
 - add a safety limiter to hailcamsnap (max_object=100000)
   - how about putting the expirer into hailcamsnap instead?
 - jettison fswebcam and invoke lib4l from Python
 - adapt hailcampack to Swift, maybe - requires staticweb middleware
